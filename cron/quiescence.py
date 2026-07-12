"""Canonical cron quiescence broker contracts.

This module owns the process-independent dispatch result and HTTP mapping.  The
broker, admission ledger and process census are added below the contracts so
all cron execution surfaces share one implementation and one durable state
root.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence
import uuid

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None
try:
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None


DISPATCH_RESULT_SCHEMA = "hermes.cron.dispatch-result.v1"
PROTOCOL_VERSION = 1


class DispatchStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    COMPLETED = "COMPLETED"
    ALREADY_RUNNING = "ALREADY_RUNNING"
    DEFERRED_QUIESCENCE = "DEFERRED_QUIESCENCE"
    QUIESCENT_BUSY = "QUIESCENT_BUSY"
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    JOB_NOT_RUNNABLE = "JOB_NOT_RUNNABLE"
    STALE_SCAN = "STALE_SCAN"
    BROKER_UNAVAILABLE = "BROKER_UNAVAILABLE"
    BROKER_PROTOCOL_MISMATCH = "BROKER_PROTOCOL_MISMATCH"
    BROKER_REQUIRED = "BROKER_REQUIRED"
    RESERVATION_FAILED = "RESERVATION_FAILED"
    SUBMIT_FAILED = "SUBMIT_FAILED"
    UNKNOWN = "UNKNOWN"


_ACCEPTED = frozenset({DispatchStatus.ACCEPTED, DispatchStatus.COMPLETED})
_RETRYABLE = frozenset(
    {
        DispatchStatus.DEFERRED_QUIESCENCE,
        DispatchStatus.QUIESCENT_BUSY,
        DispatchStatus.STALE_SCAN,
        DispatchStatus.BROKER_UNAVAILABLE,
        DispatchStatus.SUBMIT_FAILED,
    }
)
_HARD_STOP = frozenset(
    {
        DispatchStatus.BROKER_PROTOCOL_MISMATCH,
        DispatchStatus.BROKER_REQUIRED,
        DispatchStatus.RESERVATION_FAILED,
        DispatchStatus.UNKNOWN,
    }
)


@dataclass(frozen=True)
class DispatchResult:
    status: DispatchStatus
    job_id: str
    mode: str
    request_id: str
    attempt_token: Optional[str] = None
    attempt_token_sha256: Optional[str] = None
    run_token: Optional[str] = None
    run_success: Optional[bool] = None
    completion_next_run_at: Optional[str] = None
    schema: str = field(default=DISPATCH_RESULT_SCHEMA, init=False)
    accepted: bool = field(init=False)
    completed: bool = field(init=False)
    retryable: bool = field(init=False)

    def __post_init__(self) -> None:
        try:
            status = DispatchStatus(self.status)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unknown dispatch status: {self.status!r}") from exc
        object.__setattr__(self, "status", status)
        if self.mode not in {"ticker", "provider", "immediate"}:
            raise ValueError(f"invalid dispatch mode: {self.mode!r}")
        completed = status is DispatchStatus.COMPLETED
        if completed != (self.run_success is not None):
            raise ValueError("run_success must be non-null exactly for COMPLETED")
        if self.completion_next_run_at is not None and (
            not completed or type(self.completion_next_run_at) is not str
        ):
            raise ValueError(
                "completion_next_run_at must be a string only for COMPLETED"
            )
        object.__setattr__(self, "accepted", status in _ACCEPTED)
        object.__setattr__(self, "completed", completed)
        object.__setattr__(self, "retryable", status in _RETRYABLE)

    def __bool__(self) -> bool:
        return self.accepted

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value["status"] = self.status.value
        return value


@dataclass(frozen=True)
class HTTPDispatchResponse:
    status_code: int
    body: Dict[str, Any]


def dispatch_result_to_http(result: DispatchResult) -> HTTPDispatchResponse:
    """Map every dispatch status to the shared FastAPI/aiohttp contract."""
    status = result.status
    if status in _ACCEPTED:
        code = 202
    elif status in {
        DispatchStatus.ALREADY_RUNNING,
        DispatchStatus.JOB_NOT_FOUND,
        DispatchStatus.JOB_NOT_RUNNABLE,
    }:
        code = 200
    elif status is DispatchStatus.STALE_SCAN:
        code = 409
    else:
        code = 503

    body = result.to_dict()
    body["hard_stop"] = status in _HARD_STOP
    if result.retryable:
        body["retry_guidance"] = "Retry this dispatch explicitly after the reported contention clears."
    return HTTPDispatchResponse(status_code=code, body=body)


# Descriptive aliases for callers/tests that prefer a verb-first name.
map_dispatch_result_http = dispatch_result_to_http


REQUEST_SCHEMA = "hermes.cron.dispatch-request.v1"
RESPONSE_SCHEMA = "hermes.cron.dispatch-response.v1"
BARRIER_REQUEST_SCHEMA = "hermes.cron.quiescence-request.v1"
BARRIER_ACK_SCHEMA = "hermes.cron.quiescence-ack.v1"
QUIESCENT_EXEC_EVIDENCE_SCHEMA = "hermes.cron.quiescent-exec-evidence.v1"
MAX_EVIDENCE_BYTES = 1024 * 1024


def _atomic_json_write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}-", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


TRANSPORT_KEY_SCHEMA = "hermes.cron.transport-key.v1"


def _canonical_auth_bytes(payload: Mapping[str, Any]) -> bytes:
    body = {key: value for key, value in payload.items() if key != "auth_tag"}
    return json.dumps(
        body, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _signed_transport_payload(
    payload: Mapping[str, Any], key: bytes, *, domain: str
) -> Dict[str, Any]:
    result = dict(payload)
    result["auth_tag"] = hmac.new(
        key,
        domain.encode("ascii") + b"\0" + _canonical_auth_bytes(result),
        hashlib.sha256,
    ).hexdigest()
    return result


def _verify_transport_payload(
    payload: Mapping[str, Any], key: bytes, *, domain: str
) -> bool:
    tag = str(payload.get("auth_tag") or "")
    expected = hmac.new(
        key,
        domain.encode("ascii") + b"\0" + _canonical_auth_bytes(payload),
        hashlib.sha256,
    ).hexdigest()
    return bool(tag) and hmac.compare_digest(tag, expected)


def _request_payload_sha256(payload: Mapping[str, Any]) -> str:
    body = {
        key: value
        for key, value in payload.items()
        if key not in {"auth_tag", "request_sha256"}
    }
    encoded = json.dumps(
        body, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _transport_key_path(profile_home: Path | str) -> Path:
    return Path(profile_home).resolve() / "cron" / "quiescence" / "transport-key.json"


def _read_transport_key(profile_home: Path | str) -> bytes:
    payload = json.loads(_transport_key_path(profile_home).read_text(encoding="utf-8"))
    if payload.get("schema") != TRANSPORT_KEY_SCHEMA:
        raise RuntimeError("invalid cron transport key schema")
    key = bytes.fromhex(str(payload.get("key_hex") or ""))
    if len(key) != 32:
        raise RuntimeError("invalid cron transport key length")
    return key


def _ensure_transport_key(profile_home: Path | str) -> bytes:
    path = _transport_key_path(profile_home)
    if path.exists():
        return _read_transport_key(profile_home)
    key = os.urandom(32)
    _atomic_json_write(
        path,
        {"schema": TRANSPORT_KEY_SCHEMA, "key_hex": key.hex()},
    )
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return key


def _transport_failure(status: DispatchStatus, job_id: str, mode: str, request_id: str) -> DispatchResult:
    return DispatchResult(status=status, job_id=str(job_id), mode=mode, request_id=request_id)


def _load_ready_owners(root: Path) -> list[Dict[str, Any]]:
    records: list[Dict[str, Any]] = []
    for path in sorted((root / "owners").glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("ready") is True and payload.get("broker_only") is True:
            records.append(payload)
    return records


def _process_identity_is_live(identity: Mapping[str, Any]) -> bool:
    try:
        import psutil

        pid = int(identity.get("pid", -1))
        expected = float(identity.get("create_time", -1.0))
        actual = float(psutil.Process(pid).create_time())
        return abs(actual - expected) <= 0.01
    except Exception:
        return False


def _native_current_pid() -> int:
    if os.name == "nt":
        import ctypes

        return int(ctypes.windll.kernel32.GetCurrentProcessId())
    return int(os.getpid())


def current_process_identity() -> tuple[int, float]:
    import psutil

    pid = _native_current_pid()
    return pid, float(psutil.Process(pid).create_time())


def _current_process_create_time() -> float:
    return current_process_identity()[1]


def inspect_quiescence(profile_home: Path | str) -> Dict[str, Any]:
    """Return a read-only, token-free summary of canonical cron state."""
    root = Path(profile_home).resolve() / "cron" / "quiescence"
    owner_summaries = []
    errors = []
    try:
        owner_paths = sorted((root / "owners").glob("*.json"))
    except Exception as exc:
        owner_paths = []
        errors.append(f"owners:{type(exc).__name__}")
    for path in owner_paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            identity = payload.get("owner") or {}
            command_kind = str(payload.get("command_kind") or "")
            if command_kind not in (
                DISPATCH_CAPABLE_COMMAND_KINDS | KNOWN_NON_DISPATCH_COMMAND_KINDS
            ):
                command_kind = "UNKNOWN_COMMAND"
            argv_hash = str(payload.get("argv_sha256") or "")
            if len(argv_hash) != 64 or any(
                char not in "0123456789abcdef" for char in argv_hash.casefold()
            ):
                argv_hash = None
            profile_hash = str(payload.get("profile_home_hash") or "")
            if len(profile_hash) != 64 or any(
                char not in "0123456789abcdef" for char in profile_hash.casefold()
            ):
                profile_hash = None
            owner_summaries.append(
                {
                    "pid": identity.get("pid"),
                    "create_time": identity.get("create_time"),
                    "protocol_version": payload.get("protocol_version"),
                    "profile_home_hash": profile_hash,
                    "command_kind": command_kind,
                    "argv_sha256": argv_hash,
                    "ready": payload.get("ready") is True,
                    "live": _process_identity_is_live(identity),
                }
            )
        except Exception as exc:
            errors.append(f"owner_record:{type(exc).__name__}")
    ledger_summary = None
    ledger_path = root / "owner-ledger.json"
    if ledger_path.exists():
        try:
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            ledger_summary = {
                "generation": ledger.get("generation"),
                "ready": ledger.get("ready") is True,
                "active_count": len(ledger.get("active_by_job_id") or {}),
                "entry_count": len(ledger.get("entries") or {}),
                "publish_error": (
                    _bounded_redacted_output(str(ledger.get("publish_error")))[:512]
                    if ledger.get("publish_error")
                    else None
                ),
            }
        except Exception as exc:
            errors.append(f"ledger:{type(exc).__name__}")
    return {
        "schema": "hermes.cron.quiescence-inspect.v1",
        "profile_home_sha256": profile_home_sha256(profile_home),
        "owner_count": len(owner_summaries),
        "ready_owner_count": sum(item["ready"] for item in owner_summaries),
        "owners": owner_summaries,
        "ledger": ledger_summary,
        "pending_request_count": len(list((root / "requests").glob("*.json"))),
        "pending_response_count": len(list((root / "responses").glob("*.json"))),
        "completion_hook_record_count": len(
            list((root / "completion-hooks").glob("*.json"))
        ),
        "errors": errors,
    }


def _request_dispatch_via_filesystem(
    job_id: str,
    *,
    mode: str,
    profile_home: Path | str,
    timeout: float,
) -> DispatchResult:
    request_id = uuid.uuid4().hex
    root = Path(profile_home).resolve() / "cron" / "quiescence"
    try:
        transport_key = _read_transport_key(profile_home)
        ready_records = _load_ready_owners(root)
    except Exception:
        return _transport_failure(DispatchStatus.UNKNOWN, job_id, mode, request_id)
    if len(ready_records) > 1:
        return _transport_failure(DispatchStatus.UNKNOWN, job_id, mode, request_id)
    if not ready_records:
        return _transport_failure(DispatchStatus.BROKER_UNAVAILABLE, job_id, mode, request_id)
    owner_record = ready_records[0]
    owner = owner_record.get("owner")
    owner_epoch = str(owner_record.get("owner_epoch") or "")
    profile_hash = profile_home_sha256(profile_home)
    if (
        owner_record.get("schema") != "hermes.cron.owner-registration.v1"
        or int(owner_record.get("protocol_version", -1)) != PROTOCOL_VERSION
        or not isinstance(owner, dict)
        or len(owner_epoch) != 64
        or owner.get("profile_home_hash") != profile_hash
        or owner_record.get("profile_home_hash") != profile_hash
        or not _verify_transport_payload(
            owner_record, transport_key, domain="owner"
        )
    ):
        return _transport_failure(DispatchStatus.UNKNOWN, job_id, mode, request_id)
    if not _process_identity_is_live(owner):
        return _transport_failure(
            DispatchStatus.BROKER_UNAVAILABLE, job_id, mode, request_id
        )

    requests_dir = root / "requests"
    responses_dir = root / "responses"
    request_path = requests_dir / f"{request_id}.json"
    response_path = responses_dir / f"{request_id}.json"
    nonce = uuid.uuid4().hex
    try:
        caller_pid, caller_create_time = current_process_identity()
    except Exception:
        return _transport_failure(DispatchStatus.UNKNOWN, job_id, mode, request_id)
    expires_at = time.time() + max(0.1, min(5.0, float(timeout) + 0.1))
    request_payload = {
        "schema": REQUEST_SCHEMA,
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "nonce": nonce,
        "caller_pid": caller_pid,
        "caller_create_time": caller_create_time,
        "owner": owner,
        "owner_epoch": owner_epoch,
        "profile_home_sha256": profile_hash,
        "job_id": str(job_id),
        "mode": mode,
        "issued_at": time.time(),
        "expires_at": expires_at,
        "status": "REQUESTED",
    }
    request_payload["request_sha256"] = _request_payload_sha256(request_payload)
    request_payload = _signed_transport_payload(
        request_payload, transport_key, domain="dispatch-request"
    )
    try:
        _atomic_json_write(request_path, request_payload)
        deadline = time.monotonic() + max(0.0, float(timeout))
        while time.monotonic() <= deadline:
            if response_path.exists():
                try:
                    response = json.loads(response_path.read_text(encoding="utf-8"))
                except FileNotFoundError:
                    continue
                response_path.unlink(missing_ok=True)
                current_owners = _load_ready_owners(root)
                owner_still_canonical = (
                    len(current_owners) == 1
                    and current_owners[0].get("owner") == owner
                    and current_owners[0].get("owner_epoch") == owner_epoch
                    and _verify_transport_payload(
                        current_owners[0], transport_key, domain="owner"
                    )
                )
                if (
                    response.get("schema") != RESPONSE_SCHEMA
                    or response.get("request_id") != request_id
                    or response.get("nonce") != nonce
                    or int(response.get("protocol_version", -1)) != PROTOCOL_VERSION
                    or response.get("caller_pid") != caller_pid
                    or response.get("caller_create_time") != caller_create_time
                    or response.get("owner") != owner
                    or response.get("owner_epoch") != owner_epoch
                    or response.get("profile_home_sha256") != profile_hash
                    or response.get("request_sha256")
                    != request_payload.get("request_sha256")
                    or response.get("expires_at") != expires_at
                    or time.time() > expires_at
                    or not _verify_transport_payload(
                        response, transport_key, domain="dispatch-response"
                    )
                    or not owner_still_canonical
                    or not _process_identity_is_live(owner)
                ):
                    return _transport_failure(
                        DispatchStatus.UNKNOWN, job_id, mode, request_id
                    )
                return DispatchResult(
                    status=response.get("status", "UNKNOWN"),
                    job_id=str(job_id),
                    mode=mode,
                    request_id=request_id,
                )
            time.sleep(0.005)
        return _transport_failure(
            DispatchStatus.BROKER_UNAVAILABLE, job_id, mode, request_id
        )
    except Exception:
        return _transport_failure(DispatchStatus.UNKNOWN, job_id, mode, request_id)
    finally:
        request_path.unlink(missing_ok=True)
        response_path.unlink(missing_ok=True)


def request_broker_dispatch(
    job_id: str,
    *,
    mode: str,
    broker: Any = None,
    due_job: Any = None,
    profile_home: Path | str | None = None,
    timeout: float = 1.25,
) -> DispatchResult:
    """Request admission from the canonical broker without a local fallback.

    Gateway lifecycle wiring supplies ``broker`` (or a transport-backed proxy)
    once the canonical owner is ready. Callers outside that owner receive a
    structured, falsy result; they must never claim or execute the job locally.
    ``due_job`` lets the broker-owned ticker pass its read-only scan snapshot so
    admission is based on exactly the observation that made the job due.
    """
    if mode not in {"ticker", "provider", "immediate"}:
        raise ValueError(f"invalid dispatch mode: {mode!r}")
    request_id = uuid.uuid4().hex
    if broker is None:
        if profile_home is not None:
            return _request_dispatch_via_filesystem(
                job_id,
                mode=mode,
                profile_home=profile_home,
                timeout=timeout,
            )
        return DispatchResult(
            status=DispatchStatus.BROKER_UNAVAILABLE,
            job_id=str(job_id),
            mode=mode,
            request_id=request_id,
        )
    owner = getattr(broker, "owner_identity", None)
    if getattr(owner, "protocol_version", PROTOCOL_VERSION) != PROTOCOL_VERSION:
        return DispatchResult(
            status=DispatchStatus.BROKER_PROTOCOL_MISMATCH,
            job_id=str(job_id),
            mode=mode,
            request_id=request_id,
        )
    if due_job is None:
        from cron.jobs import scan_due_jobs_read_only, scan_job_for_dispatch_read_only

        if mode == "ticker":
            due_job = next(
                (item for item in scan_due_jobs_read_only().jobs if item.job_id == job_id),
                None,
            )
        else:
            due_job = scan_job_for_dispatch_read_only(job_id)
    if due_job is None:
        from cron.jobs import get_job

        status = (
            DispatchStatus.JOB_NOT_FOUND
            if get_job(job_id) is None
            else DispatchStatus.JOB_NOT_RUNNABLE
        )
        return DispatchResult(
            status=status,
            job_id=str(job_id),
            mode=mode,
            request_id=request_id,
        )
    try:
        return broker.admit(due_job, mode=mode)
    except Exception:
        return DispatchResult(
            status=DispatchStatus.UNKNOWN,
            job_id=str(job_id),
            mode=mode,
            request_id=request_id,
        )


# ---------------------------------------------------------------------------
# Admission lock, owner identity and durable in-process ledger
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OwnerIdentity:
    pid: int
    create_time: float
    profile_home_hash: str
    protocol_version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def profile_home_sha256(profile_home: Path | str) -> str:
    canonical = str(Path(profile_home).resolve()).replace("\\", "/").casefold()
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


DISPATCH_CAPABLE_COMMAND_KINDS = frozenset(
    {
        "CANONICAL_GATEWAY_RUN",
        "BARE_GATEWAY_DEFAULT_RUN",
        "GATEWAY_RESTART_RUNTIME_CAPABLE",
        "DASHBOARD_SERVER",
        "HEADLESS_SERVE_BACKEND",
        "DESKTOP_RUNTIME_CHAIN_WITH_SERVE_CHILD",
        "INTERACTIVE_AGENT_BARE_CHAT_TUI_CLI_CONSOLE",
        "ONE_SHOT_AGENT",
        "TUI_GATEWAY_SLASH_WORKER",
        "CRON_TICK",
        "CRON_RUN",
    }
)
KNOWN_NON_DISPATCH_COMMAND_KINDS = frozenset(
    {
        "GATEWAY_STATUS_QUERY_CLIENT",
        "GATEWAY_START_DETACHED_CLIENT",
        "GATEWAY_STOP_CLIENT",
        "GATEWAY_INSTALL_UNINSTALL_CLIENT",
        "CRON_MANAGEMENT_NON_EXECUTION",
        "DASHBOARD_STATUS_STOP_CLIENT",
        "DESKTOP_ELECTRON_GPU_RENDERER_UTILITY_CHILD",
        "EXACT_PROCESS_CENSUS_COLLECTOR",
        "CLI_HELP_VERSION_STATUS",
        "WRAPPER_PARENT_MATCHED_TO_CLASSIFIED_CHILD",
    }
)
PROCESS_CLASSIFICATIONS = frozenset(
    {
        "BROKER_OWNER",
        "REGISTERED_BROKER_ONLY",
        "LEGACY_DISPATCH_CAPABLE_UNREGISTERED",
        "KNOWN_NON_DISPATCH",
        "UNKNOWN_HERMES",
        "OTHER_HOME",
        "NON_HERMES",
    }
)


@dataclass(frozen=True)
class CommandClassification:
    command_kind: str
    dispatch_capable: bool
    known_non_dispatch: bool


def _command_tail(argv: Sequence[str]) -> list[str]:
    raw = [str(value) for value in argv]
    lowered = [value.casefold() for value in raw]
    for index, value in enumerate(lowered[:-1]):
        if value == "-m" and lowered[index + 1] == "hermes_cli.main":
            return raw[index + 2 :]
    if raw:
        executable = Path(raw[0]).name.casefold()
        if executable in {"hermes", "hermes.exe", "hermes-script.py"}:
            return raw[1:]
    return []


def classify_hermes_command(
    argv: Sequence[str], *, platform: str = os.name
) -> CommandClassification:
    """Classify exact versioned Hermes command kinds without a broad safe default."""
    raw = [str(value) for value in argv]
    lowered = [value.casefold() for value in raw]
    if any(value == "tui_gateway.slash_worker" for value in lowered):
        kind = "TUI_GATEWAY_SLASH_WORKER"
    else:
        tail = _command_tail(raw)
        cmd = [value.casefold() for value in tail]
        if not tail and raw and Path(raw[0]).name.casefold() not in {
            "hermes",
            "hermes.exe",
            "hermes-script.py",
        }:
            kind = "UNKNOWN_COMMAND"
        elif not cmd or cmd[0] in {"chat", "console"} or any(
            flag in cmd for flag in {"--tui", "--cli"}
        ):
            kind = "INTERACTIVE_AGENT_BARE_CHAT_TUI_CLI_CONSOLE"
        elif any(flag in cmd for flag in {"-z", "--oneshot"}):
            kind = "ONE_SHOT_AGENT"
        elif cmd[0] == "gateway":
            verb = cmd[1] if len(cmd) > 1 else ""
            if not verb:
                kind = "BARE_GATEWAY_DEFAULT_RUN"
            elif verb == "run":
                kind = "CANONICAL_GATEWAY_RUN"
            elif verb == "restart":
                kind = "GATEWAY_RESTART_RUNTIME_CAPABLE"
            elif verb == "status":
                kind = "GATEWAY_STATUS_QUERY_CLIENT"
            elif verb == "start" and platform.casefold().startswith("win"):
                kind = "GATEWAY_START_DETACHED_CLIENT"
            elif verb == "stop":
                kind = "GATEWAY_STOP_CLIENT"
            elif verb in {"install", "uninstall"}:
                kind = "GATEWAY_INSTALL_UNINSTALL_CLIENT"
            else:
                kind = "UNKNOWN_COMMAND"
        elif cmd[0] in {"dashboard", "serve"}:
            if any(flag in cmd for flag in {"--status", "--stop"}):
                kind = "DASHBOARD_STATUS_STOP_CLIENT"
            else:
                kind = "DASHBOARD_SERVER" if cmd[0] == "dashboard" else "HEADLESS_SERVE_BACKEND"
        elif cmd[0] in {"desktop", "gui"}:
            kind = "DESKTOP_RUNTIME_CHAIN_WITH_SERVE_CHILD"
        elif cmd[0] == "cron":
            verb = cmd[1] if len(cmd) > 1 else ""
            if verb == "tick":
                kind = "CRON_TICK"
            elif verb == "run":
                kind = "CRON_RUN"
            elif verb == "quiescence" and len(cmd) == 3 and cmd[2] == "inspect":
                kind = "CRON_MANAGEMENT_NON_EXECUTION"
            elif verb in {
                "list", "create", "add", "edit", "pause", "resume",
                "remove", "rm", "delete", "status",
            }:
                kind = "CRON_MANAGEMENT_NON_EXECUTION"
            else:
                kind = "UNKNOWN_COMMAND"
        elif cmd[0] in {"help", "version", "status", "--help", "--version", "-h", "-v"}:
            kind = "CLI_HELP_VERSION_STATUS"
        else:
            kind = "UNKNOWN_COMMAND"
    return CommandClassification(
        command_kind=kind,
        dispatch_capable=kind in DISPATCH_CAPABLE_COMMAND_KINDS,
        known_non_dispatch=kind in KNOWN_NON_DISPATCH_COMMAND_KINDS,
    )


def _argv_sha256(argv: Sequence[str]) -> str:
    encoded = json.dumps(
        [str(value) for value in argv], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ProcessCensusEntry:
    pid: int
    create_time: float
    ppid: Optional[int]
    classification: str
    command_kind: str
    argv_sha256: str
    hard_stop_reasons: tuple[str, ...] = ()

    def to_evidence(self) -> Dict[str, Any]:
        return {
            "pid": self.pid,
            "create_time": self.create_time,
            "ppid": self.ppid,
            "classification": self.classification,
            "command_kind": self.command_kind,
            "argv_sha256": self.argv_sha256,
            "hard_stop_reasons": list(self.hard_stop_reasons),
        }


@dataclass(frozen=True)
class ProcessCensus:
    entries: tuple[ProcessCensusEntry, ...]
    hard_stop_reasons: tuple[str, ...]
    complete: bool = True
    schema: str = field(default="hermes.cron.process-census.v1", init=False)

    @property
    def activation_safe(self) -> bool:
        if not self.complete or self.hard_stop_reasons:
            return False
        owners = [entry for entry in self.entries if entry.classification == "BROKER_OWNER"]
        if len(owners) != 1:
            return False
        return all(
            entry.classification
            not in {"LEGACY_DISPATCH_CAPABLE_UNREGISTERED", "UNKNOWN_HERMES"}
            for entry in self.entries
        )

    def to_evidence(self) -> Dict[str, Any]:
        return {
            "schema": self.schema,
            "complete": self.complete,
            "activation_safe": self.activation_safe,
            "hard_stop_reasons": list(self.hard_stop_reasons),
            "entries": [entry.to_evidence() for entry in self.entries],
        }


def _row_home_affinity(row: Mapping[str, Any], profile_home: Path) -> bool:
    if row.get("profile_home_hash") == profile_home_sha256(profile_home):
        return True
    target = str(profile_home.resolve()).replace("\\", "/").casefold()
    for key in ("exe", "cwd"):
        value = str(row.get(key) or "").replace("\\", "/").casefold()
        if value and (value.startswith(target) or target in value):
            return True
    return False


def _windows_process_sid(pid: int) -> Optional[str]:
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.LocalFree.argtypes = [wintypes.HLOCAL]
    kernel32.LocalFree.restype = wintypes.HLOCAL
    advapi32.OpenProcessToken.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi32.GetTokenInformation.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    advapi32.ConvertSidToStringSidW.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(wintypes.LPWSTR),
    ]
    process = kernel32.OpenProcess(0x1000, False, int(pid))
    if not process:
        raise OSError(ctypes.get_last_error(), "OpenProcess failed")
    token = wintypes.HANDLE()
    try:
        if not advapi32.OpenProcessToken(process, 0x0008, ctypes.byref(token)):
            raise OSError(ctypes.get_last_error(), "OpenProcessToken failed")
        needed = wintypes.DWORD(0)
        advapi32.GetTokenInformation(token, 1, None, 0, ctypes.byref(needed))
        if not needed.value:
            raise OSError(ctypes.get_last_error(), "GetTokenInformation size failed")
        buffer = ctypes.create_string_buffer(needed.value)
        if not advapi32.GetTokenInformation(
            token, 1, buffer, needed.value, ctypes.byref(needed)
        ):
            raise OSError(ctypes.get_last_error(), "GetTokenInformation failed")
        sid_pointer = ctypes.c_void_p.from_buffer(buffer).value
        rendered = wintypes.LPWSTR()
        if not advapi32.ConvertSidToStringSidW(sid_pointer, ctypes.byref(rendered)):
            raise OSError(ctypes.get_last_error(), "ConvertSidToStringSidW failed")
        try:
            return str(rendered.value)
        finally:
            kernel32.LocalFree(rendered)
    finally:
        if token:
            kernel32.CloseHandle(token)
        kernel32.CloseHandle(process)


def build_live_process_snapshot_provider(
    profile_home: Path | str,
    owner_identity: OwnerIdentity,
    *,
    owner_command_kind: str = "CANONICAL_GATEWAY_RUN",
) -> tuple[Callable[[], Iterable[Mapping[str, Any]]], str]:
    """Build a two-pass psutil source without retaining raw environment data."""
    import psutil

    from hermes_constants import _get_platform_default_hermes_home

    home = Path(profile_home).resolve()
    home_hash = profile_home_sha256(home)
    default_home = _get_platform_default_hermes_home().resolve()
    home_is_default = home == default_home
    current_username = str(psutil.Process(owner_identity.pid).username())
    try:
        current_sid = _windows_process_sid(owner_identity.pid)
    except Exception:
        current_sid = None

    def snapshot() -> Iterable[Mapping[str, Any]]:
        rows = []
        for process in psutil.process_iter(
            ["pid", "ppid", "create_time", "username", "name"]
        ):
            info = dict(process.info)
            pid = int(info.get("pid", -1))
            process_name = str(info.get("name") or "").casefold()
            row: Dict[str, Any] = {
                "pid": pid,
                "ppid": info.get("ppid"),
                "create_time": info.get("create_time"),
                "username": info.get("username"),
                "name": info.get("name"),
            }
            if current_sid:
                try:
                    row["sid"] = _windows_process_sid(pid)
                except Exception as exc:
                    if pid == owner_identity.pid or process_name.startswith("hermes"):
                        row["error"] = type(exc).__name__
                        rows.append(row)
                    continue
                if row.get("sid") != current_sid:
                    continue
            elif str(info.get("username") or "") != current_username:
                if (
                    not info.get("username")
                    and (pid == owner_identity.pid or process_name.startswith("hermes"))
                ):
                    row["error"] = "IDENTITY_UNAVAILABLE"
                    rows.append(row)
                continue
            try:
                row["argv"] = process.cmdline()
                row["exe"] = process.exe()
                row["cwd"] = process.cwd()
            except (psutil.AccessDenied, psutil.NoSuchProcess) as exc:
                name = str(info.get("name") or "").casefold()
                if pid == owner_identity.pid or name.startswith("hermes"):
                    row["error"] = type(exc).__name__
                    rows.append(row)
                continue
            try:
                env_home = (process.environ() or {}).get("HERMES_HOME")
                if env_home and profile_home_sha256(env_home) == home_hash:
                    row["profile_home_hash"] = home_hash
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                pass
            if pid == owner_identity.pid:
                row["profile_home_hash"] = home_hash
                row["runtime_attested_command_kind"] = owner_command_kind
            command = classify_hermes_command(row.get("argv") or (), platform=sys.platform)
            affinity = _row_home_affinity(row, home)
            process_name = str(row.get("name") or "").casefold()
            executable_name = Path(str(row.get("exe") or "")).name.casefold()
            looks_like_hermes_executable = (
                process_name.startswith("hermes")
                or executable_name.startswith("hermes")
            )
            if (
                pid == owner_identity.pid
                or affinity
                or (
                    home_is_default
                    and (
                        command.dispatch_capable
                        or command.known_non_dispatch
                        or looks_like_hermes_executable
                    )
                )
            ):
                rows.append(row)
        return rows

    snapshot.current_sid = current_sid  # type: ignore[attr-defined]
    snapshot.current_username = current_username  # type: ignore[attr-defined]
    return snapshot, current_username


def collect_process_census(
    *,
    profile_home: Path | str,
    owner_identity: OwnerIdentity,
    registry_records: Iterable[Mapping[str, Any]],
    snapshot_provider: Callable[[], Iterable[Mapping[str, Any]]],
    current_sid: Optional[str] = None,
    current_username: Optional[str] = None,
    platform: str = os.name,
) -> ProcessCensus:
    """Perform an injectable two-pass census and fail closed on every ambiguity."""
    home = Path(profile_home).resolve()
    home_hash = profile_home_sha256(home)
    registry = [dict(record) for record in registry_records]
    reasons: list[str] = []
    entries: list[ProcessCensusEntry] = []
    try:
        first_rows = [dict(row) for row in snapshot_provider()]
        second_rows = [dict(row) for row in snapshot_provider()]
    except Exception as exc:
        reason = f"INCOMPLETE_ENUMERATION:{type(exc).__name__}"
        return ProcessCensus((), (reason,), complete=False)

    first = {int(row["pid"]): row for row in first_rows if row.get("pid") is not None}
    second = {int(row["pid"]): row for row in second_rows if row.get("pid") is not None}
    registry_by_identity = {
        (int(record.get("pid", -1)), float(record.get("create_time", -1.0))): record
        for record in registry
    }
    observed_identities: set[tuple[int, float]] = set()
    owner_key = (owner_identity.pid, owner_identity.create_time)

    for pid in sorted(set(first) | set(second)):
        before = first.get(pid)
        after = second.get(pid)
        if before is None:
            reason = f"BIRTH_DURING_CENSUS:{pid}"
            reasons.append(reason)
            row = after or {"pid": pid, "create_time": -1.0}
            entries.append(
                ProcessCensusEntry(pid, float(row.get("create_time", -1.0)), row.get("ppid"),
                                   "UNKNOWN_HERMES", "UNKNOWN_COMMAND", _argv_sha256(row.get("argv") or ()), (reason,))
            )
            continue
        if after is None:
            reason = f"DISAPPEARED_UNCONFIRMED:{pid}"
            reasons.append(reason)
            entries.append(
                ProcessCensusEntry(pid, float(before.get("create_time", -1.0)), before.get("ppid"),
                                   "UNKNOWN_HERMES", "UNKNOWN_COMMAND", _argv_sha256(before.get("argv") or ()), (reason,))
            )
            continue
        before_ct = float(before.get("create_time", -1.0))
        after_ct = float(after.get("create_time", -1.0))
        merged = {**before, **{key: value for key, value in after.items() if value is not None}}
        if before_ct != after_ct:
            reason = f"PID_REUSE:{pid}:{before_ct}:{after_ct}"
            reasons.append(reason)
            entries.append(ProcessCensusEntry(pid, after_ct, merged.get("ppid"), "UNKNOWN_HERMES",
                                              "UNKNOWN_COMMAND", _argv_sha256(merged.get("argv") or ()), (reason,)))
            continue
        identity = (pid, before_ct)
        observed_identities.add(identity)
        error = str(after.get("error") or before.get("error") or "")
        if error:
            label = "ACCESS_DENIED" if "accessdenied" in error.replace("_", "").casefold() else "PROCESS_READ_ERROR"
            reason = f"{label}:{pid}"
            reasons.append(reason)
            entries.append(ProcessCensusEntry(pid, before_ct, merged.get("ppid"), "UNKNOWN_HERMES",
                                              "UNKNOWN_COMMAND", _argv_sha256(merged.get("argv") or ()), (reason,)))
            continue
        sid = merged.get("sid")
        username = merged.get("username")
        same_user = (current_sid is not None and sid == current_sid) or (
            current_sid is None and current_username is not None and username == current_username
        ) or (current_sid is None and current_username is None)
        command = classify_hermes_command(merged.get("argv") or (), platform=platform)
        attested_kind = str(merged.get("runtime_attested_command_kind") or "")
        if identity == owner_key and attested_kind in DISPATCH_CAPABLE_COMMAND_KINDS:
            command = CommandClassification(attested_kind, True, False)
        affinity = _row_home_affinity(merged, home)
        record = registry_by_identity.get(identity)
        entry_reasons: list[str] = []
        if not same_user:
            classification = "NON_HERMES"
        elif identity == (owner_identity.pid, owner_identity.create_time):
            if owner_identity.profile_home_hash != home_hash or not affinity or not command.dispatch_capable:
                classification = "UNKNOWN_HERMES"
                entry_reasons.append(f"OWNER_IDENTITY_CONFLICT:{pid}")
            else:
                classification = "BROKER_OWNER"
        elif record is not None:
            record_kind = str(record.get("command_kind") or "")
            valid = (
                record.get("profile_home_hash") == home_hash
                and bool(record.get("broker_only"))
                and int(record.get("protocol_version", -1)) == PROTOCOL_VERSION
                and record_kind == command.command_kind
                and affinity
            )
            if valid and command.dispatch_capable:
                classification = "REGISTERED_BROKER_ONLY"
            elif record.get("profile_home_hash") != home_hash and not affinity:
                classification = "OTHER_HOME"
            else:
                classification = "UNKNOWN_HERMES"
                entry_reasons.append(f"REGISTRY_OS_CONFLICT:{pid}")
        elif command.dispatch_capable:
            classification = "LEGACY_DISPATCH_CAPABLE_UNREGISTERED"
            entry_reasons.append(f"UNREGISTERED_DISPATCH_CAPABLE:{pid}")
        elif command.known_non_dispatch:
            classification = "KNOWN_NON_DISPATCH"
        elif affinity or command.command_kind != "UNKNOWN_COMMAND":
            classification = "UNKNOWN_HERMES"
            entry_reasons.append(f"AMBIGUOUS_HERMES_AFFINITY:{pid}")
        else:
            classification = "NON_HERMES"
        reasons.extend(entry_reasons)
        entries.append(ProcessCensusEntry(pid, before_ct, merged.get("ppid"), classification,
                                          command.command_kind, _argv_sha256(merged.get("argv") or ()), tuple(entry_reasons)))

    for identity, record in registry_by_identity.items():
        if identity in observed_identities:
            continue
        if record.get("profile_home_hash") != home_hash:
            continue
        pid, create_time = identity
        reason = f"REGISTRY_ONLY_DEAD:{pid}:{create_time}"
        reasons.append(reason)
        entries.append(ProcessCensusEntry(pid, create_time, None, "UNKNOWN_HERMES",
                                          str(record.get("command_kind") or "UNKNOWN_COMMAND"),
                                          str(record.get("argv_sha256") or ""), (reason,)))

    owner_count = sum(entry.classification == "BROKER_OWNER" for entry in entries)
    if owner_count != 1:
        reasons.append(f"BROKER_OWNER_COUNT:{owner_count}")
    return ProcessCensus(tuple(entries), tuple(dict.fromkeys(reasons)), complete=True)


_ADMISSION_PROCESS_GUARD = threading.Lock()
_ADMISSION_PROCESS_LOCKS: Dict[str, threading.Lock] = {}


def _process_admission_lock(path: Path) -> threading.Lock:
    key = str(path.resolve()).casefold()
    with _ADMISSION_PROCESS_GUARD:
        return _ADMISSION_PROCESS_LOCKS.setdefault(key, threading.Lock())


class AdmissionLock:
    """Cross-process exclusive lock that gates only new cron admissions."""

    def __init__(
        self, profile_home: Path | str, *, lock_name: str = "admission.lock"
    ):
        self.profile_home = Path(profile_home).resolve()
        if not lock_name or Path(lock_name).name != lock_name:
            raise ValueError("lock_name must be one filename")
        self.path = self.profile_home / "cron" / "quiescence" / lock_name
        self._process_lock = _process_admission_lock(self.path)
        self._fd = None
        self._held = False

    def acquire(self, timeout: float) -> bool:
        if self._held:
            raise RuntimeError("admission lock is not re-entrant")
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            if self._process_lock.acquire(blocking=False):
                break
            if time.monotonic() >= deadline:
                return False
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fd = open(self.path, "a+b")
            self._fd.seek(0, os.SEEK_END)
            if self._fd.tell() == 0:
                self._fd.write(b"0")
                self._fd.flush()
            while True:
                try:
                    self._fd.seek(0)
                    if fcntl is not None:
                        fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    elif msvcrt is not None:
                        msvcrt.locking(self._fd.fileno(), msvcrt.LK_NBLCK, 1)
                    self._held = True
                    return True
                except (OSError, IOError):
                    if time.monotonic() >= deadline:
                        self._fd.close()
                        self._fd = None
                        self._process_lock.release()
                        return False
                    time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
        except BaseException:
            if self._fd is not None:
                self._fd.close()
                self._fd = None
            self._process_lock.release()
            raise

    def release(self) -> None:
        if not self._held:
            return
        try:
            if self._fd is not None:
                try:
                    self._fd.seek(0)
                    if fcntl is not None:
                        fcntl.flock(self._fd, fcntl.LOCK_UN)
                    elif msvcrt is not None:
                        msvcrt.locking(self._fd.fileno(), msvcrt.LK_UNLCK, 1)
                finally:
                    self._fd.close()
        finally:
            self._fd = None
            self._held = False
            self._process_lock.release()

    def __enter__(self) -> "AdmissionLock":
        if not self.acquire(0.0):
            raise TimeoutError("cron admission lock is busy")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


@dataclass(frozen=True)
class QuiescentExecResult:
    status: str
    exit_code: int
    timed_out: bool
    evidence_path: str
    stdout: str = ""
    stderr: str = ""


def canonical_argv_sha256(argv: Sequence[str]) -> str:
    encoded = json.dumps(
        [str(item) for item in argv],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_quiescent_argv(argv: Sequence[str]) -> tuple[str, ...]:
    values = tuple(str(item) for item in argv)
    if not values:
        raise ValueError("quiescent-exec requires argv after --")
    executable = Path(values[0])
    if not executable.is_absolute():
        raise ValueError("quiescent-exec executable must be absolute")
    forbidden = ("&&", "||", "$(`", "$(", "`", ";", "|", "\n", "\r", ">", "<")
    secret_names = (
        "token",
        "secret",
        "password",
        "passwd",
        "api-key",
        "api_key",
        "authorization",
        "cookie",
        "credential",
    )
    try:
        from agent.redact import redact_sensitive_text
    except Exception:  # pragma: no cover - import is part of the main install
        redact_sensitive_text = None
    for index, value in enumerate(values):
        if any(marker in value for marker in forbidden):
            raise ValueError(f"quiescent-exec rejects shell syntax in argv[{index}]")
        lowered = value.casefold()
        if lowered.startswith(tuple(f"--{name}" for name in secret_names)):
            raise ValueError("quiescent-exec rejects credential-like arguments")
        if index == 0 and "=" in value and not executable.exists():
            raise ValueError("quiescent-exec rejects env-assignment prefixes")
        if redact_sensitive_text is not None:
            redacted = redact_sensitive_text(value, force=True)
            if redacted != value:
                raise ValueError("quiescent-exec rejects secret-like argv")
    return values


def _minimal_child_env(profile_home: Path) -> Dict[str, str]:
    allowed = (
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "PATH",
        "HOME",
        "TEMP",
        "TMP",
    )
    env = {key: os.environ[key] for key in allowed if os.environ.get(key)}
    env["HERMES_HOME"] = str(profile_home)
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def _bounded_redacted_output(data: bytes | str | None) -> str:
    if isinstance(data, bytes):
        text = data.decode("utf-8", errors="replace")
    else:
        text = str(data or "")
    try:
        from agent.redact import redact_sensitive_text

        text = redact_sensitive_text(text, force=True)
    except Exception:
        text = "[redaction unavailable]"
    raw = text.encode("utf-8")[:MAX_EVIDENCE_BYTES]
    return raw.decode("utf-8", errors="ignore")


def _await_quiescence_ack(
    profile_home: Path,
    *,
    argv_sha256: str,
    argv_count: int,
    executable_sha256: str,
    child_timeout: float,
    deadline: float,
    transport_paths: Optional[Dict[str, Path]] = None,
) -> Dict[str, Any]:
    root = profile_home / "cron" / "quiescence"
    transport_key = _read_transport_key(profile_home)
    owners = _load_ready_owners(root)
    if len(owners) != 1:
        raise RuntimeError("expected exactly one ready cron owner")
    owner_record = owners[0]
    if (
        owner_record.get("profile_home_hash") != profile_home_sha256(profile_home)
        or not _verify_transport_payload(
            owner_record, transport_key, domain="owner"
        )
    ):
        raise RuntimeError("cron owner profile/auth mismatch")
    owner = owner_record.get("owner")
    owner_epoch = str(owner_record.get("owner_epoch") or "")
    if (
        not isinstance(owner, dict)
        or len(owner_epoch) != 64
        or not _process_identity_is_live(owner)
    ):
        raise RuntimeError("cron owner is not live")
    request_id = uuid.uuid4().hex
    nonce = uuid.uuid4().hex
    request_path = root / "requests" / f"{request_id}.json"
    response_path = root / "responses" / f"{request_id}.json"
    if transport_paths is not None:
        transport_paths["request"] = request_path
        transport_paths["response"] = response_path
    caller_pid, caller_create_time = current_process_identity()
    issued_at = time.time()
    expires_at = issued_at + max(
        0.1, min(5.0, deadline - time.monotonic())
    )
    request_payload = {
        "schema": BARRIER_REQUEST_SCHEMA,
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "nonce": nonce,
        "caller_pid": caller_pid,
        "caller_create_time": caller_create_time,
        "owner": owner,
        "owner_epoch": owner_epoch,
        "profile_home_sha256": profile_home_sha256(profile_home),
        "issued_at": issued_at,
        "expires_at": expires_at,
        "argv_sha256": argv_sha256,
        "argv_count": int(argv_count),
        "executable_sha256": executable_sha256,
        "executable_path_class": "absolute",
        "child_timeout": float(child_timeout),
    }
    request_payload["request_sha256"] = _request_payload_sha256(request_payload)
    request_payload = _signed_transport_payload(
        request_payload, transport_key, domain="barrier-request"
    )
    _atomic_json_write(request_path, request_payload)
    while time.monotonic() < deadline:
        if response_path.exists():
            try:
                response = json.loads(response_path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                continue
            if not (
                response.get("schema") == BARRIER_ACK_SCHEMA
                and int(response.get("protocol_version", -1)) == PROTOCOL_VERSION
                and response.get("request_id") == request_id
                and response.get("nonce") == nonce
                and response.get("owner") == owner
                and response.get("owner_epoch") == owner_epoch
                and response.get("caller_pid") == caller_pid
                and response.get("caller_create_time") == caller_create_time
                and response.get("request_sha256")
                == request_payload.get("request_sha256")
                and response.get("expires_at") == expires_at
                and time.time() <= expires_at
                and _verify_transport_payload(
                    response, transport_key, domain="barrier-ack"
                )
                and response.get("ready") is True
            ):
                raise RuntimeError("invalid or drifted cron owner ACK")
            ack_generation = int(response.get("generation", -1))
            break
        time.sleep(0.01)
    else:
        raise TimeoutError("timed out waiting for cron owner ACK")

    ledger_path = root / "owner-ledger.json"
    while time.monotonic() < deadline:
        current_owners = _load_ready_owners(root)
        if (
            len(current_owners) != 1
            or current_owners[0].get("owner") != owner
            or current_owners[0].get("owner_epoch") != owner_epoch
            or not _verify_transport_payload(
                current_owners[0], transport_key, domain="owner"
            )
            or not _process_identity_is_live(owner)
        ):
            raise RuntimeError("cron owner drift while waiting for empty ledger")
        try:
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError("missing or malformed cron owner ledger") from exc
        if (
            ledger.get("owner") != owner
            or ledger.get("owner_epoch") != owner_epoch
            or ledger.get("ready") is not True
            or not _verify_transport_payload(
                ledger, transport_key, domain="owner-ledger"
            )
        ):
            raise RuntimeError("cron ledger owner/ready/auth mismatch")
        if ledger.get("publish_error"):
            raise RuntimeError("cron ledger publish error")
        if int(ledger.get("generation", -1)) < ack_generation:
            raise RuntimeError("cron ledger generation regressed")
        if not ledger.get("active_by_job_id") and not ledger.get("entries"):
            return {
                "owner": owner,
                "request_id": request_id,
                "nonce": nonce,
                "generation": int(ledger.get("generation", -1)),
                "request_path": request_path,
                "response_path": response_path,
            }
        time.sleep(0.01)
    raise TimeoutError("timed out waiting for empty cron ledger")


class _BoundedPipeCapture:
    def __init__(self, stream: Any, limit: int = MAX_EVIDENCE_BYTES):
        self.stream = stream
        self.limit = int(limit)
        self.buffer = bytearray()
        self.error: Optional[BaseException] = None
        self.thread = threading.Thread(target=self._drain, daemon=True)

    def _drain(self) -> None:
        try:
            while True:
                chunk = self.stream.read(65536)
                if not chunk:
                    break
                remaining = self.limit - len(self.buffer)
                if remaining > 0:
                    self.buffer.extend(chunk[:remaining])
        except BaseException as exc:
            self.error = exc
        finally:
            try:
                self.stream.close()
            except Exception:
                pass

    def start(self) -> None:
        self.thread.start()

    def finish(self, timeout: float = 5.0) -> bytes:
        self.thread.join(timeout=max(0.0, float(timeout)))
        if self.thread.is_alive():
            raise RuntimeError("child output collector did not stop")
        if self.error is not None:
            raise RuntimeError(
                f"child output collector failed: {type(self.error).__name__}"
            )
        return bytes(self.buffer)


class _WindowsKillOnCloseJob:
    def __init__(self) -> None:
        import ctypes
        from ctypes import wintypes

        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [(name, ctypes.c_ulonglong) for name in (
                "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
                "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
            )]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        class JOBOBJECT_BASIC_ACCOUNTING_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("TotalUserTime", ctypes.c_longlong),
                ("TotalKernelTime", ctypes.c_longlong),
                ("ThisPeriodTotalUserTime", ctypes.c_longlong),
                ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
                ("TotalPageFaultCount", wintypes.DWORD),
                ("TotalProcesses", wintypes.DWORD),
                ("ActiveProcesses", wintypes.DWORD),
                ("TotalTerminatedProcesses", wintypes.DWORD),
            ]

        self._ctypes = ctypes
        self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self._kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        self._kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        self._kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD
        ]
        self._kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        self._kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        self._kernel32.QueryInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
            ctypes.c_void_p,
        ]
        self._kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        self._accounting_type = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION
        self.handle = self._kernel32.CreateJobObjectW(None, None)
        if not self.handle:
            raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")
        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = 0x00002000
        if not self._kernel32.SetInformationJobObject(
            self.handle, 9, ctypes.byref(info), ctypes.sizeof(info)
        ):
            error = ctypes.get_last_error()
            self.close()
            raise OSError(error, "SetInformationJobObject failed")

    def assign(self, process: subprocess.Popen[Any]) -> None:
        if not self._kernel32.AssignProcessToJobObject(
            self.handle, self._ctypes.c_void_p(int(process._handle))
        ):
            raise OSError(
                self._ctypes.get_last_error(), "AssignProcessToJobObject failed"
            )

    def terminate(self) -> bool:
        return bool(self._kernel32.TerminateJobObject(self.handle, 1))

    def wait_empty(self, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            info = self._accounting_type()
            if not self._kernel32.QueryInformationJobObject(
                self.handle,
                1,
                self._ctypes.byref(info),
                self._ctypes.sizeof(info),
                None,
            ):
                return False
            if info.ActiveProcesses == 0:
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.01)

    def close(self) -> None:
        if getattr(self, "handle", None):
            self._kernel32.CloseHandle(self.handle)
            self.handle = None


def _resume_suspended_windows_process(pid: int) -> None:
    import ctypes
    from ctypes import wintypes
    import psutil

    threads = psutil.Process(pid).threads()
    if len(threads) != 1:
        raise RuntimeError("suspended child did not expose exactly one initial thread")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenThread.restype = wintypes.HANDLE
    kernel32.OpenThread.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.ResumeThread.argtypes = [wintypes.HANDLE]
    kernel32.ResumeThread.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel32.OpenThread(0x0002, False, int(threads[0].id))
    if not handle:
        raise OSError(ctypes.get_last_error(), "OpenThread failed")
    try:
        if kernel32.ResumeThread(handle) == 0xFFFFFFFF:
            raise OSError(ctypes.get_last_error(), "ResumeThread failed")
    finally:
        kernel32.CloseHandle(handle)


def _terminate_owned_process(
    process: subprocess.Popen[Any], windows_job: Any = None
) -> bool:
    if os.name == "nt" and windows_job is not None:
        try:
            if not windows_job.terminate():
                return False
            process.wait(timeout=5.0)
            return process.poll() is not None and windows_job.wait_empty(5.0)
        except Exception:
            return False
    if process.poll() is not None:
        return True
    try:
        if os.name == "nt":
            import psutil

            parent = psutil.Process(process.pid)
            descendants = parent.children(recursive=True)
            for owned in descendants:
                try:
                    owned.terminate()
                except psutil.Error:
                    pass
            parent.terminate()
            _gone, alive = psutil.wait_procs([parent, *descendants], timeout=5.0)
            for owned in alive:
                try:
                    owned.kill()
                except psutil.Error:
                    pass
            _gone, alive = psutil.wait_procs(alive, timeout=5.0)
            return not alive
        os.killpg(process.pid, signal.SIGTERM)
    except Exception:
        pass
    try:
        process.wait(timeout=5.0)
        return True
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5.0)
    except Exception:
        return process.poll() is not None
    return process.poll() is not None


class _UnassignedProcessContainment:
    """Retryable containment for a child that could not enter the Job Object."""

    def __init__(self, process: subprocess.Popen[Any]) -> None:
        self.process = process

    def terminate(self) -> bool:
        if self.process.poll() is not None:
            return True
        try:
            self.process.terminate()
            self.process.wait(timeout=1.0)
        except Exception:
            try:
                self.process.kill()
                self.process.wait(timeout=1.0)
            except Exception:
                pass
        return self.process.poll() is not None

    def wait_empty(self, timeout: float = 5.0) -> bool:
        try:
            self.process.wait(timeout=max(0.0, float(timeout)))
        except Exception:
            pass
        return self.process.poll() is not None

    def close(self) -> None:
        return None


_UNCONFIRMED_CONTAINMENTS: Dict[str, Dict[str, Any]] = {}
_UNCONFIRMED_CONTAINMENTS_LOCK = threading.Lock()
_CONTAINMENT_SCHEMA = "hermes.cron.unconfirmed-containment.v1"


def _write_raw_containment_sentinel(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        {**dict(payload), "evidence_status": "UNSIGNED_FAIL_CLOSED"},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        view = memoryview(encoded)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write while persisting containment sentinel")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _persist_containment_state(state: Dict[str, Any]) -> bool:
    try:
        transport_key = state.get("transport_key")
        if transport_key is None:
            transport_key = _ensure_transport_key(state["profile"])
            state["transport_key"] = transport_key
        _atomic_json_write(
            state["record_path"],
            _signed_transport_payload(
                state["record_payload"],
                transport_key,
                domain="unconfirmed-containment",
            ),
        )
        return True
    except Exception:
        try:
            _atomic_json_write(
                state["record_path"],
                {
                    **dict(state["record_payload"]),
                    "evidence_status": "UNSIGNED_FAIL_CLOSED",
                },
            )
        except Exception:
            try:
                _write_raw_containment_sentinel(
                    state["record_path"], state["record_payload"]
                )
            except Exception:
                pass
        return False


def assert_no_live_unconfirmed_containments(profile_home: Path | str) -> None:
    """Hard-stop a new owner while durable evidence identifies a live child."""
    import psutil

    profile = Path(profile_home).resolve()
    directory = profile / "cron" / "quiescence" / "containments"
    if not directory.exists():
        return
    key = _read_transport_key(profile)
    for record_path in sorted(directory.glob("*.json")):
        try:
            payload = json.loads(record_path.read_text(encoding="utf-8"))
            if (
                not _verify_transport_payload(
                    payload, key, domain="unconfirmed-containment"
                )
                or payload.get("schema") != _CONTAINMENT_SCHEMA
                or payload.get("profile_home_sha256") != profile_home_sha256(profile)
                or not isinstance(payload.get("pid"), int)
                or not isinstance(payload.get("create_time"), (int, float))
            ):
                raise RuntimeError("invalid durable containment evidence")
            pid = int(payload["pid"])
            create_time = float(payload["create_time"])
            try:
                observed = float(psutil.Process(pid).create_time())
            except psutil.NoSuchProcess:
                record_path.unlink(missing_ok=True)
                continue
            if abs(observed - create_time) > 0.01:
                record_path.unlink(missing_ok=True)
                continue
            raise RuntimeError(
                f"live unconfirmed child containment blocks cron owner: pid={pid}"
            )
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError("cannot verify durable containment evidence") from exc


def retry_unconfirmed_containments_once() -> int:
    """Retry owned-tree termination and release barriers only after confirmation."""
    with _UNCONFIRMED_CONTAINMENTS_LOCK:
        items = []
        for containment_id, state in _UNCONFIRMED_CONTAINMENTS.items():
            if state.get("retrying"):
                continue
            state["retrying"] = True
            items.append((containment_id, state))
    released = 0
    for containment_id, state in items:
        if not _terminate_owned_process(state["process"], state["windows_job"]):
            with _UNCONFIRMED_CONTAINMENTS_LOCK:
                current = _UNCONFIRMED_CONTAINMENTS.get(containment_id)
                if current is state:
                    current["retrying"] = False
            continue
        with _UNCONFIRMED_CONTAINMENTS_LOCK:
            current = _UNCONFIRMED_CONTAINMENTS.pop(containment_id, None)
        if current is not state:
            continue
        try:
            state["windows_job"].close()
            record_path = state.get("record_path")
            if record_path is not None:
                Path(record_path).unlink(missing_ok=True)
        finally:
            state["admission"].release()
        released += 1
    return released


def _retain_unconfirmed_containment(
    profile: Path,
    admission: AdmissionLock,
    process: subprocess.Popen[Any],
    windows_job: Any,
) -> str:
    import psutil

    try:
        create_time: Optional[float] = float(psutil.Process(process.pid).create_time())
    except Exception:
        create_time = None
    containment_id = hashlib.sha256(
        f"{profile_home_sha256(profile)}:{process.pid}:{create_time}".encode("utf-8")
    ).hexdigest()
    record_path = (
        profile / "cron" / "quiescence" / "containments" / f"{containment_id}.json"
    )
    state = {
        "profile": profile,
        "admission": admission,
        "process": process,
        "windows_job": windows_job,
        "record_path": record_path,
        "transport_key": None,
        "persisted_signed": False,
        "record_payload": {
            "schema": _CONTAINMENT_SCHEMA,
            "profile_home_sha256": profile_home_sha256(profile),
            "pid": int(process.pid),
            "create_time": create_time,
            "status": "UNCONFIRMED",
        },
    }
    with _UNCONFIRMED_CONTAINMENTS_LOCK:
        _UNCONFIRMED_CONTAINMENTS[containment_id] = state
    state["persisted_signed"] = _persist_containment_state(state)

    def watchdog() -> None:
        while True:
            with _UNCONFIRMED_CONTAINMENTS_LOCK:
                if containment_id not in _UNCONFIRMED_CONTAINMENTS:
                    return
            if not state.get("persisted_signed"):
                state["persisted_signed"] = _persist_containment_state(state)
            if retry_unconfirmed_containments_once():
                with _UNCONFIRMED_CONTAINMENTS_LOCK:
                    if containment_id not in _UNCONFIRMED_CONTAINMENTS:
                        return
            time.sleep(0.25)

    async_supervision = False
    if record_path.exists():
        try:
            threading.Thread(
                target=watchdog,
                daemon=True,
                name=f"cron-containment-{process.pid}",
            ).start()
            async_supervision = True
        except Exception:
            pass
    if not async_supervision:
        # No durable evidence or no asynchronous supervisor exists. Keep this
        # owner and its admission barrier alive until termination is confirmed.
        while True:
            retry_unconfirmed_containments_once()
            with _UNCONFIRMED_CONTAINMENTS_LOCK:
                if containment_id not in _UNCONFIRMED_CONTAINMENTS:
                    break
            time.sleep(0.25)
    return containment_id


def execute_quiescent_child(
    argv: Sequence[str],
    *,
    expected_argv_sha256: str,
    profile_home: Path | str,
    wait_timeout: float,
    child_timeout: float,
) -> QuiescentExecResult:
    values = validate_quiescent_argv(argv)
    profile = Path(profile_home).resolve()
    computed_hash = canonical_argv_sha256(values)
    if computed_hash != str(expected_argv_sha256).casefold():
        raise ValueError("quiescent-exec argv SHA-256 mismatch")
    executable_sha256 = hashlib.sha256(values[0].encode("utf-8")).hexdigest()
    root = profile / "cron" / "quiescence"
    evidence_dir = root / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    deadline = started + max(0.0, float(wait_timeout))
    admission = AdmissionLock(profile)
    request_path: Optional[Path] = None
    response_path: Optional[Path] = None
    transport_paths: Dict[str, Path] = {}
    windows_job: Any = None
    windows_job_assigned = False
    process: Optional[subprocess.Popen[Any]] = None
    admission_retained = False
    stdout = ""
    stderr = ""
    exit_code = 70
    status = "HARD_STOP"
    timed_out = False
    request_id = uuid.uuid4().hex
    evidence_path = evidence_dir / f"quiescent-exec-{request_id}.json"
    acquired = admission.acquire(max(0.0, deadline - time.monotonic()))
    try:
        if not acquired:
            status = "QUIESCENT_BUSY"
        else:
            ack = _await_quiescence_ack(
                profile,
                argv_sha256=computed_hash,
                argv_count=len(values),
                executable_sha256=executable_sha256,
                child_timeout=child_timeout,
                deadline=deadline,
                transport_paths=transport_paths,
            )
            request_id = str(ack["request_id"])
            request_path = ack["request_path"]
            response_path = ack["response_path"]
            evidence_path = evidence_dir / f"quiescent-exec-{request_id}.json"
            if canonical_argv_sha256(values) != computed_hash:
                raise RuntimeError("argv changed before spawn")
            popen_kwargs: Dict[str, Any] = {
                "shell": False,
                "env": _minimal_child_env(profile),
                "stdout": subprocess.PIPE,
                "stderr": subprocess.PIPE,
            }
            if os.name == "nt":
                popen_kwargs["creationflags"] = (
                    subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000004
                )
                windows_job = _WindowsKillOnCloseJob()
            else:
                popen_kwargs["start_new_session"] = True
            process = subprocess.Popen(list(values), **popen_kwargs)
            try:
                if windows_job is not None:
                    windows_job.assign(process)
                    windows_job_assigned = True
            except Exception:
                if not _terminate_owned_process(process):
                    if windows_job is not None:
                        windows_job.close()
                    fallback_containment = _UnassignedProcessContainment(process)
                    admission_retained = True
                    _retain_unconfirmed_containment(
                        profile, admission, process, fallback_containment
                    )
                    windows_job = None
                    status = "UNKNOWN_HARD_STOP"
                    exit_code = 70
                raise
            stdout_capture = _BoundedPipeCapture(process.stdout)
            stderr_capture = _BoundedPipeCapture(process.stderr)
            stdout_capture.start()
            stderr_capture.start()
            if windows_job is not None:
                _resume_suspended_windows_process(process.pid)
            try:
                process.wait(timeout=max(0.001, float(child_timeout)))
                root_exit_code = int(process.returncode)
                if windows_job is not None:
                    if not windows_job.terminate() or not windows_job.wait_empty(5.0):
                        admission_retained = True
                        _retain_unconfirmed_containment(
                            profile, admission, process, windows_job
                        )
                        windows_job = None
                        exit_code = 70
                        status = "UNKNOWN_HARD_STOP"
                    else:
                        exit_code = root_exit_code
                        status = "COMPLETED" if exit_code == 0 else "CHILD_FAILED"
                else:
                    exit_code = root_exit_code
                    status = "COMPLETED" if exit_code == 0 else "CHILD_FAILED"
            except subprocess.TimeoutExpired:
                timed_out = True
                terminated = _terminate_owned_process(process, windows_job)
                exit_code = 124 if terminated else 70
                status = "TIMEOUT" if terminated else "UNKNOWN_HARD_STOP"
                if not terminated and windows_job is not None:
                    admission_retained = True
                    _retain_unconfirmed_containment(
                        profile, admission, process, windows_job
                    )
                    windows_job = None
            finally:
                if windows_job is not None:
                    windows_job.close()
                    windows_job = None
            out = stdout_capture.finish()
            err = stderr_capture.finish()
            stdout = _bounded_redacted_output(out)
            stderr = _bounded_redacted_output(err)
    except Exception as exc:
        if (
            process is not None
            and windows_job is not None
            and windows_job_assigned
            and not admission_retained
        ):
            if not _terminate_owned_process(process, windows_job):
                admission_retained = True
                _retain_unconfirmed_containment(
                    profile, admission, process, windows_job
                )
                windows_job = None
                status = "UNKNOWN_HARD_STOP"
                exit_code = 70
        stderr = _bounded_redacted_output(f"{type(exc).__name__}: {exc}")
        if status != "UNKNOWN_HARD_STOP":
            status = "HARD_STOP"
            exit_code = 70
    finally:
        evidence = {
            "schema": QUIESCENT_EXEC_EVIDENCE_SCHEMA,
            "request_id": request_id,
            "status": status,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "argv_sha256": computed_hash,
            "argv_count": len(values),
            "executable_sha256": executable_sha256,
            "executable_path_class": "absolute",
            "child_timeout": float(child_timeout),
            "stdout": stdout,
            "stderr": stderr,
            "stdout_sha256": hashlib.sha256(stdout.encode("utf-8")).hexdigest(),
            "stderr_sha256": hashlib.sha256(stderr.encode("utf-8")).hexdigest(),
        }
        try:
            _atomic_json_write(evidence_path, evidence)
        except Exception as exc:
            if status != "UNKNOWN_HARD_STOP":
                status = "HARD_STOP"
                exit_code = 70
            stderr = _bounded_redacted_output(
                f"evidence write failed: {type(exc).__name__}: {exc}"
            )
        finally:
            if windows_job is not None:
                try:
                    windows_job.close()
                except Exception:
                    pass
            cleanup_paths = set(transport_paths.values())
            if request_path is not None:
                cleanup_paths.add(request_path)
            if response_path is not None:
                cleanup_paths.add(response_path)
            for cleanup_path in cleanup_paths:
                try:
                    cleanup_path.unlink(missing_ok=True)
                except Exception:
                    pass
            if acquired and not admission_retained:
                admission.release()
    return QuiescentExecResult(
        status=status,
        exit_code=exit_code,
        timed_out=timed_out,
        evidence_path=str(evidence_path),
        stdout=stdout,
        stderr=stderr,
    )


class _BrokerCapability:
    __slots__ = ("_nonce",)

    def __init__(self, nonce: object):
        self._nonce = nonce


class RetryableCompletionHookError(RuntimeError):
    """Provider effect is idempotent and should be retried from durable journal."""


@dataclass(frozen=True)
class RunOutcome:
    success: bool
    error: Optional[str] = None
    delivery_error: Optional[str] = None


class CronBroker:
    """The one process-local execution owner for a profile home."""

    _DEFAULT_LOCK_TIMEOUT = {"ticker": 0.0, "provider": 0.250, "immediate": 1.0}

    def __init__(
        self,
        *,
        profile_home: Path | str,
        owner_identity: OwnerIdentity,
        submit: Callable[[Callable[[], DispatchResult]], Any],
        runner: Callable[[Dict[str, Any]], Any],
        completion_hook: Optional[Callable[[DispatchResult], Any]] = None,
        census_snapshot_provider: Optional[
            Callable[[], Iterable[Mapping[str, Any]]]
        ] = None,
        census_current_sid: Optional[str] = None,
        census_current_username: Optional[str] = None,
        census_platform: Optional[str] = None,
    ) -> None:
        self.profile_home = Path(profile_home).resolve()
        self.owner_identity = owner_identity
        self.submit = submit
        self.runner = runner
        self.completion_hook = completion_hook
        self.census_snapshot_provider = census_snapshot_provider
        self.census_current_sid = census_current_sid
        self.census_current_username = census_current_username
        self.census_platform = census_platform or sys.platform
        self._ledger_mutex = threading.RLock()
        self._active_by_job_id: Dict[str, str] = {}
        self._entries: Dict[str, Dict[str, Any]] = {}
        self._served_request_keys: set[tuple[str, ...]] = set()
        self._generation = 0
        self._publish_error: Optional[str] = None
        self._capability_nonce = object()
        self._capability = _BrokerCapability(self._capability_nonce)
        self._ready = False
        self._registered_command_kind: Optional[str] = None
        self._registered_argv_hash: Optional[str] = None
        self._request_server_started = threading.Event()
        self.owner_epoch = uuid.uuid4().hex + uuid.uuid4().hex
        self._transport_key: Optional[bytes] = None
        self._owner_lifetime_lock = AdmissionLock(
            self.profile_home, lock_name="owner.lock"
        )
        state_root = self.profile_home / "cron" / "quiescence"
        self._ledger_path = state_root / "owner-ledger.json"
        create_stamp = str(self.owner_identity.create_time).replace(".", "-")
        self._owner_registry_path = (
            state_root / "owners" / f"{self.owner_identity.pid}-{create_stamp}.json"
        )

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def ledger_path(self) -> Path:
        return self._ledger_path

    @property
    def owner_registry_path(self) -> Path:
        return self._owner_registry_path

    @staticmethod
    def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            dir=str(path.parent), prefix=f".{path.name}-", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def _owner_payload(
        self, *, command_kind: str, argv_hash: str, ready: Optional[bool] = None
    ) -> Dict[str, Any]:
        payload = {
            "schema": "hermes.cron.owner-registration.v1",
            "owner": self.owner_identity.to_dict(),
            "owner_epoch": self.owner_epoch,
            "protocol_version": self.owner_identity.protocol_version,
            "profile_home_hash": self.owner_identity.profile_home_hash,
            "broker_only": True,
            "command_kind": command_kind,
            "argv_sha256": argv_hash,
            "ready": self._ready if ready is None else ready,
        }
        if self._transport_key is None:
            raise RuntimeError("transport key is not initialized")
        return _signed_transport_payload(payload, self._transport_key, domain="owner")

    def register_owner(self, *, command_kind: str, argv: Sequence[str]) -> Path:
        """Register exactly one unready owner under a cross-process lock."""
        if command_kind not in DISPATCH_CAPABLE_COMMAND_KINDS:
            raise ValueError(f"owner command is not dispatch-capable: {command_kind}")
        argv_hash = _argv_sha256(argv)
        if not self._owner_lifetime_lock.acquire(timeout=0.0):
            raise RuntimeError("canonical cron owner lock is already held")
        registration = AdmissionLock(
            self.profile_home, lock_name="owner-registration.lock"
        )
        if not registration.acquire(timeout=1.0):
            self._owner_lifetime_lock.release()
            raise RuntimeError("owner registration lock is busy")
        wrote_registration = False
        try:
            assert_no_live_unconfirmed_containments(self.profile_home)
            owners_dir = self._owner_registry_path.parent
            existing = list(owners_dir.glob("*.json")) if owners_dir.exists() else []
            if existing:
                raise RuntimeError(
                    "owner registration already exists; cleanup/census approval required"
                )
            if self.census_snapshot_provider is not None:
                census = collect_process_census(
                    profile_home=self.profile_home,
                    owner_identity=self.owner_identity,
                    registry_records=(),
                    snapshot_provider=self.census_snapshot_provider,
                    current_sid=self.census_current_sid,
                    current_username=self.census_current_username,
                    platform=self.census_platform,
                )
                self._atomic_write_json(
                    self.profile_home
                    / "cron"
                    / "quiescence"
                    / "process-census"
                    / "latest.json",
                    census.to_evidence(),
                )
                if not census.activation_safe:
                    reasons = ",".join(census.hard_stop_reasons) or "unsafe"
                    raise RuntimeError(f"owner registration census hard stop: {reasons}")
            self._transport_key = _ensure_transport_key(self.profile_home)
            with self._ledger_mutex:
                self._ready = False
                self._registered_command_kind = command_kind
                self._registered_argv_hash = argv_hash
                self._publish_locked()
                self._atomic_write_json(
                    self._owner_registry_path,
                    self._owner_payload(
                        command_kind=command_kind,
                        argv_hash=argv_hash,
                        ready=False,
                    ),
                )
                wrote_registration = True
        except BaseException:
            with self._ledger_mutex:
                self._ready = False
                try:
                    if wrote_registration and self._owner_registry_path.exists():
                        payload = json.loads(self._owner_registry_path.read_text(encoding="utf-8"))
                        if payload.get("owner") == self.owner_identity.to_dict():
                            self._owner_registry_path.unlink(missing_ok=True)
                except Exception:
                    pass
            self._owner_lifetime_lock.release()
            raise
        finally:
            registration.release()
        return self._owner_registry_path

    def wait_request_server_started(self, timeout: float) -> bool:
        return self._request_server_started.wait(max(0.0, float(timeout)))

    def mark_owner_ready(self) -> None:
        """Publish ready only after the request-serving thread is observable."""
        if not self._request_server_started.is_set():
            raise RuntimeError("request server is not started")
        if not self._registered_command_kind or not self._registered_argv_hash:
            raise RuntimeError("owner is not registered")
        with self._ledger_mutex:
            self._ready = True
            try:
                self._publish_locked()
                self._atomic_write_json(
                    self._owner_registry_path,
                    self._owner_payload(
                        command_kind=self._registered_command_kind,
                        argv_hash=self._registered_argv_hash,
                        ready=True,
                    ),
                )
            except BaseException:
                self._ready = False
                try:
                    self._publish_locked()
                except Exception:
                    self._publish_error = "owner_ready_rollback_publish_failed"
                raise

    def close_owner(self) -> bool:
        """Close only this exact owner identity; foreign/reused records survive."""
        result = False
        try:
            with self._ledger_mutex:
                self._ready = False
                try:
                    if not self._owner_registry_path.exists():
                        result = True
                    else:
                        payload = json.loads(
                            self._owner_registry_path.read_text(encoding="utf-8")
                        )
                        if payload.get("owner") != self.owner_identity.to_dict():
                            result = False
                        else:
                            try:
                                self._publish_locked()
                            except Exception:
                                self._publish_error = "owner_close_publish_failed"
                            self._owner_registry_path.unlink(missing_ok=True)
                            result = True
                except Exception:
                    result = False
        finally:
            self._owner_lifetime_lock.release()
        return result

    def serve_request_queue(
        self, stop_event: threading.Event, *, poll_interval: float = 0.025
    ) -> None:
        try:
            self._serve_request_queue_loop(
                stop_event, poll_interval=poll_interval
            )
        finally:
            self._request_server_started.clear()
            with self._ledger_mutex:
                self._ready = False
                try:
                    if (
                        self._transport_key is not None
                        and self._registered_command_kind
                        and self._registered_argv_hash
                    ):
                        self._atomic_write_json(
                            self._owner_registry_path,
                            self._owner_payload(
                                command_kind=self._registered_command_kind,
                                argv_hash=self._registered_argv_hash,
                                ready=False,
                            ),
                        )
                    self._publish_locked()
                except Exception:
                    self._publish_error = "request_server_exit_demote_failed"
                    try:
                        self._owner_registry_path.unlink(missing_ok=True)
                    except Exception:
                        pass

    def _serve_request_queue_loop(
        self, stop_event: threading.Event, *, poll_interval: float = 0.025
    ) -> None:
        """Serve authenticated local dispatch envelopes for the exact owner."""
        root = self.profile_home / "cron" / "quiescence"
        requests_dir = root / "requests"
        responses_dir = root / "responses"
        requests_dir.mkdir(parents=True, exist_ok=True)
        responses_dir.mkdir(parents=True, exist_ok=True)
        if self._transport_key is None:
            raise RuntimeError("transport key is not initialized")
        self._request_server_started.set()
        while not stop_event.is_set():
            if not self._ready:
                stop_event.wait(max(0.001, poll_interval))
                continue
            for request_path in sorted(requests_dir.glob("*.json")):
                if stop_event.is_set():
                    break
                request_id = request_path.stem
                mode = "provider"
                job_id = ""
                nonce = ""
                caller_pid = -1
                caller_create_time = -1.0
                profile_hash = profile_home_sha256(self.profile_home)
                expires_at = 0.0
                try:
                    payload = json.loads(request_path.read_text(encoding="utf-8"))
                    payload_request_id = str(payload.get("request_id") or "")
                    if payload.get("schema") == BARRIER_REQUEST_SCHEMA:
                        nonce = str(payload.get("nonce") or "")
                        caller_pid = int(payload.get("caller_pid", -1))
                        caller_create_time = float(
                            payload.get("caller_create_time", -1.0)
                        )
                        issued_at = float(payload.get("issued_at", 0.0))
                        expires_at = float(payload.get("expires_at", 0.0))
                        request_sha256 = str(payload.get("request_sha256") or "")
                        valid_barrier = (
                            str(payload.get("request_id") or "") == request_id
                            and int(payload.get("protocol_version", -1)) == PROTOCOL_VERSION
                            and payload.get("owner") == self.owner_identity.to_dict()
                            and payload.get("owner_epoch") == self.owner_epoch
                            and payload.get("profile_home_sha256")
                            == profile_home_sha256(self.profile_home)
                            and bool(nonce)
                            and bool(payload.get("argv_sha256"))
                            and int(payload.get("argv_count", 0)) > 0
                            and payload.get("executable_path_class") == "absolute"
                            and issued_at <= time.time() <= expires_at
                            and 0.0 < expires_at - issued_at <= 5.5
                            and request_sha256 == _request_payload_sha256(payload)
                            and _verify_transport_payload(
                                payload,
                                self._transport_key,
                                domain="barrier-request",
                            )
                            and _process_identity_is_live(
                                {
                                    "pid": caller_pid,
                                    "create_time": caller_create_time,
                                }
                            )
                        )
                        if valid_barrier:
                            request_key = (
                                self.owner_epoch,
                                str(caller_pid),
                                str(caller_create_time),
                                nonce,
                            )
                            with self._ledger_mutex:
                                if request_key in self._served_request_keys:
                                    valid_barrier = False
                                else:
                                    self._served_request_keys.add(request_key)
                        if not valid_barrier:
                            continue
                        with self._ledger_mutex:
                            snapshot = self._ledger_payload_locked()
                        response = _signed_transport_payload(
                            {
                                "schema": BARRIER_ACK_SCHEMA,
                                "protocol_version": PROTOCOL_VERSION,
                                "request_id": request_id,
                                "nonce": nonce,
                                "request_sha256": request_sha256,
                                "caller_pid": caller_pid,
                                "caller_create_time": caller_create_time,
                                "owner": self.owner_identity.to_dict(),
                                "owner_epoch": self.owner_epoch,
                                "expires_at": expires_at,
                                "ready": bool(self._ready),
                                "generation": snapshot["generation"],
                                "active_count": len(snapshot["active_by_job_id"]),
                                "publish_error": snapshot.get("publish_error"),
                            },
                            self._transport_key,
                            domain="barrier-ack",
                        )
                        _atomic_json_write(
                            responses_dir / f"{request_id}.json", response
                        )
                        continue
                    payload_request_id = str(payload.get("request_id") or "")
                    job_id = str(payload.get("job_id") or "")
                    requested_mode = str(payload.get("mode") or "")
                    nonce = str(payload.get("nonce") or "")
                    caller_pid = int(payload.get("caller_pid", -1))
                    caller_create_time = float(
                        payload.get("caller_create_time", -1.0)
                    )
                    expires_at = float(payload.get("expires_at", 0.0))
                    issued_at = float(payload.get("issued_at", 0.0))
                    request_sha256 = str(payload.get("request_sha256") or "")
                    if requested_mode in {"ticker", "provider", "immediate"}:
                        mode = requested_mode
                    valid = (
                        payload.get("schema") == REQUEST_SCHEMA
                        and payload_request_id == request_id
                        and int(payload.get("protocol_version", -1)) == PROTOCOL_VERSION
                        and payload.get("profile_home_sha256") == profile_hash
                        and payload.get("owner") == self.owner_identity.to_dict()
                        and payload.get("owner_epoch") == self.owner_epoch
                        and requested_mode == mode
                        and payload.get("status") == "REQUESTED"
                        and bool(nonce)
                        and bool(job_id)
                        and issued_at <= time.time() <= expires_at
                        and 0.0 < expires_at - issued_at <= 5.5
                        and request_sha256 == _request_payload_sha256(payload)
                        and _verify_transport_payload(
                            payload,
                            self._transport_key,
                            domain="dispatch-request",
                        )
                        and _process_identity_is_live(
                            {
                                "pid": caller_pid,
                                "create_time": caller_create_time,
                            }
                        )
                    )
                    if valid:
                        request_key = (
                            self.owner_epoch,
                            str(caller_pid),
                            str(caller_create_time),
                            nonce,
                        )
                        with self._ledger_mutex:
                            if request_key in self._served_request_keys:
                                valid = False
                            else:
                                self._served_request_keys.add(request_key)
                                if len(self._served_request_keys) > 8192:
                                    self._served_request_keys.pop()
                    if not valid:
                        continue
                    result = request_broker_dispatch(job_id, mode=mode, broker=self)
                    response = _signed_transport_payload(
                        {
                            "schema": RESPONSE_SCHEMA,
                            "protocol_version": PROTOCOL_VERSION,
                            "request_id": request_id,
                            "nonce": nonce,
                            "request_sha256": request_sha256,
                            "caller_pid": caller_pid,
                            "caller_create_time": caller_create_time,
                            "owner": self.owner_identity.to_dict(),
                            "owner_epoch": self.owner_epoch,
                            "profile_home_sha256": profile_hash,
                            "expires_at": expires_at,
                            "status": result.status.value,
                        },
                        self._transport_key,
                        domain="dispatch-response",
                    )
                    _atomic_json_write(
                        responses_dir / f"{request_id}.json", response
                    )
                except Exception:
                    pass
                finally:
                    request_path.unlink(missing_ok=True)
            stop_event.wait(max(0.001, poll_interval))

    def _ledger_payload_locked(self) -> Dict[str, Any]:
        return {
            "schema": "hermes.cron.owner-ledger.v1",
            "owner": self.owner_identity.to_dict(),
            "owner_epoch": self.owner_epoch,
            "generation": self._generation,
            "ready": self._ready,
            "active_by_job_id": dict(self._active_by_job_id),
            "entries": {token: dict(entry) for token, entry in self._entries.items()},
            "publish_error": self._publish_error,
        }

    def _publish_locked(self) -> None:
        self._generation += 1
        payload = self._ledger_payload_locked()
        if self._transport_key is not None:
            payload = _signed_transport_payload(
                payload, self._transport_key, domain="owner-ledger"
            )
        self._ledger_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            dir=str(self._ledger_path.parent), prefix=".ledger-", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self._ledger_path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def ledger_snapshot(self) -> Dict[str, Any]:
        with self._ledger_mutex:
            return json.loads(json.dumps(self._ledger_payload_locked()))

    @staticmethod
    def _busy_status(mode: str) -> DispatchStatus:
        return (
            DispatchStatus.QUIESCENT_BUSY
            if mode == "immediate"
            else DispatchStatus.DEFERRED_QUIESCENCE
        )

    def admit(self, due_job: Any, *, mode: str, lock_timeout: Optional[float] = None) -> DispatchResult:
        if mode not in self._DEFAULT_LOCK_TIMEOUT:
            raise ValueError(f"invalid dispatch mode: {mode!r}")
        request_id = uuid.uuid4().hex
        admission = AdmissionLock(self.profile_home)
        timeout = self._DEFAULT_LOCK_TIMEOUT[mode] if lock_timeout is None else lock_timeout
        if not admission.acquire(timeout):
            return DispatchResult(
                status=self._busy_status(mode), job_id=due_job.job_id,
                mode=mode, request_id=request_id,
            )

        attempt_token: Optional[str] = None
        run_token: Optional[str] = None
        receipt = None
        try:
            from cron.jobs import reserve_job_attempt, rollback_reserved_attempt

            with self._ledger_mutex:
                if due_job.job_id in self._active_by_job_id:
                    return DispatchResult(
                        status=DispatchStatus.ALREADY_RUNNING,
                        job_id=due_job.job_id,
                        mode=mode,
                        request_id=request_id,
                    )
                attempt_token = uuid.uuid4().hex
                run_token = uuid.uuid4().hex
                reservation = reserve_job_attempt(
                    due_job.job_id,
                    attempt_token,
                    run_token,
                    mode,
                    due_job.observed_job_sha256,
                    self.owner_identity.to_dict(),
                )
                if reservation.status != "RESERVED":
                    status = DispatchStatus.__members__.get(
                        reservation.status, DispatchStatus.RESERVATION_FAILED
                    )
                    return DispatchResult(
                        status=status,
                        job_id=due_job.job_id,
                        mode=mode,
                        request_id=request_id,
                    )
                receipt = reservation.receipt
                self._active_by_job_id[due_job.job_id] = run_token
                self._entries[run_token] = {
                    "job_id": due_job.job_id,
                    "attempt_token": attempt_token,
                    "run_token": run_token,
                    "mode": mode,
                    "state": "RESERVED",
                }
                try:
                    self._publish_locked()
                except Exception as exc:
                    self._publish_error = type(exc).__name__
                    self._entries[run_token]["state"] = "UNKNOWN"
                    return DispatchResult(
                        status=DispatchStatus.UNKNOWN,
                        job_id=due_job.job_id,
                        mode=mode,
                        request_id=request_id,
                    )

            def worker() -> DispatchResult:
                with self._ledger_mutex:
                    entry = self._entries.get(run_token)
                    if entry is None or entry.get("attempt_token") != attempt_token:
                        return DispatchResult(
                            status=DispatchStatus.UNKNOWN,
                            job_id=due_job.job_id,
                            mode=mode,
                            request_id=request_id,
                        )
                    entry["state"] = "COMPLETING"
                    self._publish_locked()
                result = execute_reserved_job(
                    reservation.job,
                    receipt,
                    self._capability,
                    self.runner,
                    capability_nonce=self._capability_nonce,
                    completion_hook=self.completion_hook,
                    profile_home=self.profile_home,
                    owner_epoch=self.owner_epoch,
                    request_id=request_id,
                )
                with self._ledger_mutex:
                    entry = self._entries.get(run_token)
                    if entry is not None and entry.get("attempt_token") == attempt_token:
                        if result.status is DispatchStatus.COMPLETED:
                            self._entries.pop(run_token, None)
                            if self._active_by_job_id.get(due_job.job_id) == run_token:
                                self._active_by_job_id.pop(due_job.job_id, None)
                        else:
                            entry["state"] = "UNKNOWN"
                        self._publish_locked()
                return result

            try:
                self.submit(worker)
            except Exception:
                with self._ledger_mutex:
                    entry = self._entries.get(run_token)
                    if entry is None or entry.get("attempt_token") != attempt_token:
                        return DispatchResult(
                            status=DispatchStatus.UNKNOWN, job_id=due_job.job_id,
                            mode=mode, request_id=request_id,
                        )
                    rollback = rollback_reserved_attempt(receipt)
                    if rollback.status == "ROLLED_BACK":
                        self._entries.pop(run_token, None)
                        if self._active_by_job_id.get(due_job.job_id) == run_token:
                            self._active_by_job_id.pop(due_job.job_id, None)
                        self._publish_locked()
                        return DispatchResult(
                            status=DispatchStatus.SUBMIT_FAILED,
                            job_id=due_job.job_id,
                            mode=mode,
                            request_id=request_id,
                        )
                    entry["state"] = "UNKNOWN"
                    self._publish_error = "submit_rollback_cas_mismatch"
                    self._publish_locked()
                    return DispatchResult(
                        status=DispatchStatus.UNKNOWN, job_id=due_job.job_id,
                        mode=mode, request_id=request_id,
                    )

            return DispatchResult(
                status=DispatchStatus.ACCEPTED,
                job_id=due_job.job_id,
                mode=mode,
                request_id=request_id,
                attempt_token=attempt_token,
                run_token=run_token,
            )
        finally:
            admission.release()


_COMPLETION_HOOK_SCHEMA = "hermes.cron.completion-hook-outcome.v1"
_COMPLETION_HOOK_NONTERMINAL = frozenset({"PENDING", "COMMITTED", "STARTED"})
_COMPLETION_HOOK_TERMINAL = frozenset({"SUCCEEDED", "NO_ACTION", "UNKNOWN_PARTIAL"})
_COMPLETION_HOOK_STATUSES = _COMPLETION_HOOK_NONTERMINAL | _COMPLETION_HOOK_TERMINAL


_COMPLETION_HOOK_KEYS = frozenset(
    {
        "schema",
        "job_id",
        "attempt_token_sha256",
        "reservation_post_job_sha256",
        "removes_on_completion",
        "mode",
        "request_id",
        "dispatch_status",
        "owner_epoch",
        "run_success",
        "completion_next_run_at",
        "status",
        "error",
        "auth_tag",
    }
)


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _validate_completion_hook_record(
    profile: Path,
    record_path: Path,
    payload: Mapping[str, Any],
    transport_key: bytes,
    *,
    expected: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    record = dict(payload)
    job_id = record.get("job_id")
    attempt_hash = record.get("attempt_token_sha256")
    mode = record.get("mode")
    status = record.get("status")
    canonical_path = _completion_hook_record_path(profile, job_id, attempt_hash)
    if (
        set(record) != _COMPLETION_HOOK_KEYS
        or not _verify_transport_payload(record, transport_key, domain="completion-hook")
        or record.get("schema") != _COMPLETION_HOOK_SCHEMA
        or type(job_id) is not str
        or not job_id
        or not _is_sha256(attempt_hash)
        or not _is_sha256(record.get("reservation_post_job_sha256"))
        or type(mode) is not str
        or mode not in {"ticker", "provider", "immediate"}
        or type(record.get("request_id")) is not str
        or not record.get("request_id")
        or record.get("dispatch_status") != DispatchStatus.COMPLETED.value
        or type(record.get("removes_on_completion")) is not bool
        or (
            record.get("owner_epoch") is not None
            and (type(record.get("owner_epoch")) is not str or not record.get("owner_epoch"))
        )
        or (
            status == "PENDING"
            and record.get("run_success") is not None
        )
        or (
            status != "PENDING"
            and type(record.get("run_success")) is not bool
        )
        or (
            record.get("completion_next_run_at") is not None
            and (
                type(record.get("completion_next_run_at")) is not str
                or not record.get("completion_next_run_at")
            )
        )
        or type(status) is not str
        or status not in _COMPLETION_HOOK_STATUSES
        or (
            record.get("error") is not None
            and type(record.get("error")) is not str
        )
        or type(record.get("auth_tag")) is not str
        or record_path.resolve() != canonical_path.resolve()
    ):
        raise RuntimeError("invalid completion hook journal")
    if expected is not None:
        immutable = (
            "schema",
            "job_id",
            "attempt_token_sha256",
            "reservation_post_job_sha256",
            "removes_on_completion",
            "mode",
            "request_id",
            "dispatch_status",
        )
        if any(record.get(key) != expected.get(key) for key in immutable):
            raise RuntimeError("completion hook journal identity mismatch")
    return record


def _completion_hook_record_path(
    profile: Path, job_id: str, attempt_token_sha256: str
) -> Path:
    record_id = hashlib.sha256(
        json.dumps(
            [str(job_id), str(attempt_token_sha256)], separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    return (
        profile
        / "cron"
        / "quiescence"
        / "completion-hooks"
        / f"{record_id}.json"
    )


def _write_completion_hook_record(
    record_path: Path,
    transport_key: bytes,
    payload: Mapping[str, Any],
    *,
    status: str,
    error: Optional[str] = None,
) -> None:
    value = {
        key: item
        for key, item in dict(payload).items()
        if key != "auth_tag"
    }
    value["status"] = status
    value["error"] = error
    _atomic_json_write(
        record_path,
        _signed_transport_payload(value, transport_key, domain="completion-hook"),
    )


def _invoke_durable_completion_hook(
    profile: Path,
    result: DispatchResult,
    completion_hook: Callable[[DispatchResult], Any],
    *,
    owner_epoch: Optional[str],
    transport_key: bytes,
    record_path: Path,
    base_payload: Mapping[str, Any],
) -> bool:
    lock_name = f"completion-hook-{record_path.stem}.lock"
    journal_lock = AdmissionLock(profile, lock_name=lock_name)
    if not journal_lock.acquire(timeout=5.0):
        return False
    try:
        payload = dict(base_payload)
        if record_path.exists():
            try:
                existing = json.loads(record_path.read_text(encoding="utf-8"))
                existing = _validate_completion_hook_record(
                    profile,
                    record_path,
                    existing,
                    transport_key,
                    expected=base_payload,
                )
            except Exception:
                return False
            if str(existing.get("status")) in _COMPLETION_HOOK_TERMINAL:
                return False
            payload = existing
        payload["completion_next_run_at"] = base_payload.get(
            "completion_next_run_at"
        )
        payload["owner_epoch"] = owner_epoch
        payload["dispatch_status"] = result.status.value
        payload["run_success"] = result.run_success
        try:
            _write_completion_hook_record(
                record_path, transport_key, payload, status="STARTED"
            )
        except Exception:
            return False
        hook_status = "UNKNOWN_PARTIAL"
        hook_error = None
        try:
            hook_status = "SUCCEEDED" if bool(completion_hook(result)) else "NO_ACTION"
        except RetryableCompletionHookError as exc:
            hook_status = "COMMITTED"
            hook_error = _bounded_redacted_output(
                f"{type(exc).__name__}: {exc}"
            )[:4096]
        except Exception as exc:
            hook_error = _bounded_redacted_output(
                f"{type(exc).__name__}: {exc}"
            )[:4096]
        try:
            _write_completion_hook_record(
                record_path,
                transport_key,
                payload,
                status=hook_status,
                error=hook_error,
            )
        except Exception:
            # STARTED is intentionally recoverable. Provider operations must use
            # their stable dedup key so a recovery invocation converges safely.
            pass
        return True
    finally:
        journal_lock.release()


def recover_completion_hooks(
    profile_home: Path | str,
    completion_hook: Callable[[DispatchResult], Any],
    *,
    owner_epoch: Optional[str] = None,
) -> int:
    """Retry signed PENDING/COMMITTED/STARTED hooks whose job commit is durable."""
    from cron.jobs import get_job, load_completion_proof_read_only

    profile = Path(profile_home).resolve()
    try:
        transport_key = _read_transport_key(profile)
    except Exception:
        return 0
    hook_dir = profile / "cron" / "quiescence" / "completion-hooks"
    if not hook_dir.exists():
        return 0
    recovered = 0
    for record_path in sorted(hook_dir.glob("*.json")):
        try:
            payload = json.loads(record_path.read_text(encoding="utf-8"))
            payload = _validate_completion_hook_record(
                profile, record_path, payload, transport_key
            )
            status = str(payload.get("status"))
            if status in _COMPLETION_HOOK_TERMINAL:
                continue
            if status not in {"PENDING", "COMMITTED", "STARTED"}:
                continue
            job_id = payload["job_id"]
            attempt_hash = payload["attempt_token_sha256"]
            proof = load_completion_proof_read_only(job_id, attempt_hash)
            if proof is None:
                continue
            job = get_job(job_id)
            if job is not None and (
                job.get("run_claim") is not None
                or job.get("fire_claim") is not None
            ):
                continue
            if proof.get("removed") is not True and job is None:
                continue
            if (
                status != "PENDING"
                and (
                    payload.get("run_success") is not proof.get("run_success")
                    or payload.get("completion_next_run_at")
                    != proof.get("next_run_at")
                )
            ):
                continue
            recovered_run_success = proof["run_success"]
            recovered_next_run_at = proof.get("next_run_at")
            payload = dict(payload)
            payload["completion_next_run_at"] = recovered_next_run_at
            mode = payload["mode"]
            if mode not in {"ticker", "provider", "immediate"}:
                mode = "provider"
            result = DispatchResult(
                status=DispatchStatus.COMPLETED,
                job_id=job_id,
                mode=mode,
                request_id=str(payload.get("request_id") or record_path.stem),
                attempt_token_sha256=attempt_hash,
                run_success=recovered_run_success,
                completion_next_run_at=payload.get("completion_next_run_at"),
            )
            if _invoke_durable_completion_hook(
                profile,
                result,
                completion_hook,
                owner_epoch=owner_epoch,
                transport_key=transport_key,
                record_path=record_path,
                base_payload=payload,
            ):
                recovered += 1
        except Exception:
            continue
    return recovered


def serve_completion_hook_recovery_loop(
    stop_event: threading.Event,
    profile_home: Path | str,
    completion_hook: Callable[[DispatchResult], Any],
    *,
    owner_epoch: Optional[str] = None,
    interval: float = 5.0,
) -> None:
    """Retry durable nonterminal hooks online at a bounded rate until shutdown."""
    delay = max(0.01, float(interval))
    while not stop_event.wait(delay):
        try:
            recover_completion_hooks(
                profile_home,
                completion_hook,
                owner_epoch=owner_epoch,
            )
        except Exception:
            continue


def execute_reserved_job(
    reserved_job: Dict[str, Any],
    receipt: Any,
    capability: Any,
    runner: Callable[[Dict[str, Any]], Any],
    *,
    capability_nonce: Any = None,
    completion_hook: Optional[Callable[[DispatchResult], Any]] = None,
    profile_home: Path | str | None = None,
    owner_epoch: Optional[str] = None,
    request_id: Optional[str] = None,
) -> DispatchResult:
    """Execute/complete only when invoked with this broker's private capability."""
    job_id = str((reserved_job or {}).get("id") or "")
    mode = getattr(receipt, "mode", "provider")
    if (
        not isinstance(capability, _BrokerCapability)
        or capability_nonce is None
        or capability._nonce is not capability_nonce
        or receipt is None
    ):
        return DispatchResult(
            status=DispatchStatus.BROKER_REQUIRED,
            job_id=job_id,
            mode=mode,
            request_id=request_id or uuid.uuid4().hex,
        )

    from cron.jobs import complete_reserved_attempt

    hook_context: Optional[tuple[Path, bytes, Path, Dict[str, Any]]] = None
    attempt_hash = hashlib.sha256(
        str(receipt.attempt_token or "").encode("utf-8")
    ).hexdigest()
    effective_request_id = request_id or uuid.uuid4().hex
    if completion_hook is not None and profile_home is not None:
        profile = Path(profile_home).resolve()
        try:
            transport_key = _read_transport_key(profile)
            record_path = _completion_hook_record_path(
                profile, job_id, attempt_hash
            )
            repeat = reserved_job.get("repeat") or {}
            repeat_times = repeat.get("times")
            removes_on_completion = bool(
                (reserved_job.get("schedule") or {}).get("kind") == "once"
                and repeat_times is not None
                and repeat_times > 0
                and int(repeat.get("completed", 0)) >= int(repeat_times)
            )
            base_payload = {
                "schema": _COMPLETION_HOOK_SCHEMA,
                "job_id": job_id,
                "attempt_token_sha256": attempt_hash,
                "reservation_post_job_sha256": receipt.post_job_sha256,
                "removes_on_completion": removes_on_completion,
                "mode": mode,
                "request_id": effective_request_id,
                "dispatch_status": DispatchStatus.COMPLETED.value,
                "owner_epoch": owner_epoch,
                "run_success": None,
                "completion_next_run_at": reserved_job.get("next_run_at"),
                "status": "PENDING",
                "error": None,
            }
            if record_path.exists():
                existing = json.loads(record_path.read_text(encoding="utf-8"))
                existing = _validate_completion_hook_record(
                    profile,
                    record_path,
                    existing,
                    transport_key,
                    expected=base_payload,
                )
                if str(existing.get("status")) not in _COMPLETION_HOOK_NONTERMINAL:
                    raise RuntimeError("completion hook attempt already terminal")
                base_payload = existing
            else:
                _write_completion_hook_record(
                    record_path,
                    transport_key,
                    base_payload,
                    status="PENDING",
                )
            hook_context = (profile, transport_key, record_path, base_payload)
        except Exception:
            return DispatchResult(
                status=DispatchStatus.UNKNOWN,
                job_id=job_id,
                mode=mode,
                request_id=effective_request_id,
                attempt_token=receipt.attempt_token,
                attempt_token_sha256=attempt_hash,
                run_token=receipt.run_token,
            )

    try:
        raw_outcome = runner(reserved_job)
        if isinstance(raw_outcome, RunOutcome):
            outcome = raw_outcome
        else:
            outcome = RunOutcome(success=bool(raw_outcome))
    except Exception as exc:
        outcome = RunOutcome(success=False, error=str(exc))

    committed = complete_reserved_attempt(
        job_id,
        receipt.attempt_token,
        success=outcome.success,
        error=outcome.error,
        delivery_error=outcome.delivery_error,
    )
    if committed.status != "COMPLETED":
        return DispatchResult(
            status=DispatchStatus.UNKNOWN,
            job_id=job_id,
            mode=mode,
            request_id=request_id or uuid.uuid4().hex,
            attempt_token=receipt.attempt_token,
            run_token=receipt.run_token,
        )
    result = DispatchResult(
        status=DispatchStatus.COMPLETED,
        job_id=job_id,
        mode=mode,
        request_id=effective_request_id,
        attempt_token=receipt.attempt_token,
        attempt_token_sha256=attempt_hash,
        run_token=receipt.run_token,
        run_success=outcome.success,
        completion_next_run_at=committed.next_run_at,
    )
    if hook_context is not None:
        profile, transport_key, record_path, base_payload = hook_context
        base_payload = dict(base_payload)
        base_payload["run_success"] = outcome.success
        base_payload["completion_next_run_at"] = committed.next_run_at
        base_payload["dispatch_status"] = DispatchStatus.COMPLETED.value
        try:
            _write_completion_hook_record(
                record_path,
                transport_key,
                base_payload,
                status="COMMITTED",
            )
        except Exception:
            # PENDING remains recoverable only when jobs state proves this attempt
            # committed; no uncommitted attempt is promoted by journal state alone.
            pass
        hook_context = (profile, transport_key, record_path, base_payload)
    if completion_hook is not None:
        if hook_context is None:
            try:
                completion_hook(result)
            except Exception:
                pass
        else:
            profile, transport_key, record_path, base_payload = hook_context
            _invoke_durable_completion_hook(
                profile,
                result,
                completion_hook,
                owner_epoch=owner_epoch,
                transport_key=transport_key,
                record_path=record_path,
                base_payload=base_payload,
            )
    return result
