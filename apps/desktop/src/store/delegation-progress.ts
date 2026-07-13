export interface DelegationChildProgress {
  taskIndex: number
  status: string
  phase: string
  heartbeatAt: number | null
  heartbeatAgeSeconds: number | null
  currentTool: string | null
  apiCalls: number | null
  budgetUsed: number | null
  budgetMax: number | null
}

export interface DelegationProgress {
  id: string
  status: string
  phase: string
  totalCount: number
  finishedCount: number
  completedCount: number
  failedCount: number
  runningCount: number
  progressPercent: number
  heartbeatAt: number | null
  heartbeatAgeSeconds: number | null
  stale: boolean
  children: DelegationChildProgress[]
}

export interface DelegationStatusSnapshot {
  schemaVersion: number
  processInstanceId: string
  processLocal: boolean
  snapshotAt: number | null
  delegations: DelegationProgress[]
}

const DELEGATION_PROGRESS_SCHEMA_VERSION = 1

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null
}

function text(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

function numberOrNull(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function count(value: unknown): number {
  const parsed = numberOrNull(value)

  return parsed === null ? 0 : Math.max(0, Math.floor(parsed))
}

const TERMINAL_STATUSES = new Set(['completed', 'failed', 'interrupted', 'unknown'])
const ACTIVE_PHASES = new Set(['queued', 'running', 'starting', 'model', 'tool', 'waiting_model', 'waiting_peer'])

const STATUS_ALIASES: Record<string, string> = {
  queued: 'queued',
  running: 'running',
  completed: 'completed',
  failed: 'failed',
  error: 'failed',
  timeout: 'failed',
  interrupted: 'interrupted',
  canceled: 'interrupted',
  cancelled: 'interrupted',
  unknown: 'unknown'
}

function normalizeStatus(value: unknown): string {
  const status = text(value).trim().toLowerCase()

  return STATUS_ALIASES[status] ?? 'unknown'
}

function normalizePhase(value: unknown, status: string): string {
  if (TERMINAL_STATUSES.has(status)) {
    return status
  }

  const phase = text(value).trim().toLowerCase()

  return ACTIVE_PHASES.has(phase) ? phase : status
}

function parseChild(value: unknown): DelegationChildProgress | null {
  const raw = record(value)

  if (!raw) {
    return null
  }

  const status = normalizeStatus(raw.status)

  return {
    taskIndex: count(raw.task_index),
    status,
    phase: normalizePhase(raw.phase, status),
    heartbeatAt: numberOrNull(raw.heartbeat_at),
    heartbeatAgeSeconds: numberOrNull(raw.heartbeat_age_seconds),
    currentTool: text(raw.current_tool) || null,
    apiCalls: numberOrNull(raw.api_calls),
    budgetUsed: numberOrNull(raw.budget_used),
    budgetMax: numberOrNull(raw.budget_max)
  }
}

function parseDelegation(value: unknown): DelegationProgress | null {
  const raw = record(value)
  const id = text(raw?.delegation_id)

  if (!raw || !id) {
    return null
  }

  const totalCount = count(raw.total_count)
  const finishedCount = Math.min(totalCount, count(raw.finished_count))
  const completedCount = Math.min(finishedCount, count(raw.completed_count))
  const failedCount = Math.min(Math.max(0, finishedCount - completedCount), count(raw.failed_count))
  const status = normalizeStatus(raw.status)

  const children = Array.isArray(raw.children)
    ? raw.children.map(parseChild).filter((child): child is DelegationChildProgress => child !== null)
    : []

  return {
    id,
    status,
    phase: normalizePhase(raw.phase, status),
    totalCount,
    finishedCount,
    completedCount,
    failedCount,
    runningCount: Math.max(0, totalCount - finishedCount),
    progressPercent: totalCount > 0 ? Math.round((finishedCount / totalCount) * 100) : 0,
    heartbeatAt: numberOrNull(raw.heartbeat_at),
    heartbeatAgeSeconds: numberOrNull(raw.heartbeat_age_seconds),
    stale: raw.stale === true,
    children
  }
}

export function parseDelegationStatus(value: unknown): DelegationStatusSnapshot {
  const raw = record(value)
  const processInstanceId = text(raw?.process_instance_id).trim()

  if (
    !raw ||
    raw.schema_version !== DELEGATION_PROGRESS_SCHEMA_VERSION ||
    raw.process_local !== true ||
    !processInstanceId ||
    !Array.isArray(raw.delegations)
  ) {
    throw new Error('Invalid delegation progress payload')
  }

  const delegations = raw.delegations
    .map(parseDelegation)
    .filter((item): item is DelegationProgress => item !== null)

  return {
    schemaVersion: DELEGATION_PROGRESS_SCHEMA_VERSION,
    processInstanceId,
    processLocal: true,
    snapshotAt: numberOrNull(raw.snapshot_at),
    delegations
  }
}
