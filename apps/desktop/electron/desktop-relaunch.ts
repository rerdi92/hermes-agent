import fs from 'node:fs'

const DEFAULT_BACKEND_SHUTDOWN_TIMEOUT_MS = 120_000

export interface BackendChild {
  exitCode: null | number
  once: (event: 'exit', listener: (code: null | number, signal: null | string) => void) => unknown
  removeListener: (event: 'exit', listener: (code: null | number, signal: null | string) => void) => unknown
  signalCode: null | string
}

export interface BackendShutdownTarget {
  child: BackendChild | null
  label: string
  markerPath: string
}

type BackendShutdownReason = 'abnormal-exit' | 'exited' | 'marker-write-failed' | 'timeout' | 'unsupported'

export interface BackendShutdownResult {
  label: string
  ok: boolean
  reason: BackendShutdownReason
}

interface ShutdownOptions {
  removeMarker?: (markerPath: string) => Promise<void>
  timeoutMs?: number
  writeMarker?: (markerPath: string) => Promise<void>
}

interface ShutdownGroupResult {
  ok: boolean
  results: BackendShutdownResult[]
}

interface RelaunchPreflightResult {
  ok: boolean
  reason?: string
}

export interface DesktopRelaunchPreflightState {
  activeTerminalCount: number
  bootstrapActive: boolean
  handoffActive: boolean
  hasConnectionPromise: boolean
  hasLocalBackendChild: boolean
  primaryConnectionMode: null | 'local' | 'remote'
  unresolvedPoolCount: number
  updateInFlight: boolean
}

interface RelaunchCoordinatorOptions {
  getTargets: () => BackendShutdownTarget[]
  preflight: () => RelaunchPreflightResult
  quit: () => void
  relaunch: () => void
  shutdownTargets?: (targets: BackendShutdownTarget[]) => Promise<ShutdownGroupResult>
}

export interface RelaunchResult {
  ok: boolean
  reason: string
}

export interface GracefulRelaunchCoordinator {
  (): Promise<RelaunchResult>
  readonly relaunchIssued: boolean
}

export interface DesktopRelaunchLifecycle {
  readonly active: boolean
  readonly draining: boolean
  assertOperationAllowed: (operation: string) => void
  assertRelaunchNotActive: (operation: string) => void
  markRelaunching: () => void
  request: () => Promise<RelaunchResult>
  runOperation: <T>(operation: string, callback: (assertActive: () => void) => Promise<T> | T) => Promise<T>
}

interface DesktopRelaunchLifecycleOptions {
  isHandoffActive?: () => boolean
  isRelaunchPending?: () => boolean
}

export interface DesktopStartupFence {
  assertCurrent: (generation: number) => void
  begin: () => number
  invalidate: () => void
  isCurrent: (generation: number) => boolean
}

export interface DesktopBackendStartupOwner {
  assertCurrent: (assertLease?: () => void) => void
  invalidate: () => void
  isCurrent: () => boolean
}

export interface DesktopOwnedStartupEntry<T> {
  connectionPromise: Promise<T> | null
  starting: boolean
  startupOwner: DesktopBackendStartupOwner | null
}

interface DesktopOwnedStartupOptions<K, T, E extends DesktopOwnedStartupEntry<T>> {
  activeLease?: (() => void) | null
  beforeStart?: (assertLease: () => void) => Promise<void> | void
  createEntry: () => E
  entries: Map<K, E>
  key: K
  label: string
  lifecycle: DesktopRelaunchLifecycle
  operation: string
  start: (entry: E, assertCurrent: () => void) => Promise<T>
}

export function decideDesktopRelaunchPreflight(
  state: DesktopRelaunchPreflightState
): RelaunchPreflightResult {
  if (state.bootstrapActive) {
    return { ok: false, reason: 'bootstrap-active' }
  }

  if (state.updateInFlight) {
    return { ok: false, reason: 'update-active' }
  }

  if (state.handoffActive) {
    return { ok: false, reason: 'handoff-active' }
  }

  if (state.activeTerminalCount > 0) {
    return { ok: false, reason: 'active-terminals' }
  }

  if (state.unresolvedPoolCount > 0) {
    return { ok: false, reason: 'pool-starting' }
  }

  if (state.hasConnectionPromise && state.primaryConnectionMode === null) {
    return { ok: false, reason: 'backend-starting' }
  }

  return { ok: true }
}

export function createDesktopRelaunchLifecycle(
  requestRelaunch: () => Promise<RelaunchResult>,
  options: DesktopRelaunchLifecycleOptions = {}
): DesktopRelaunchLifecycle {
  let phase: 'draining' | 'idle' | 'relaunching' = 'idle'
  let inFlight: Promise<RelaunchResult> | null = null
  let operationCount = 0
  const handoffActive = () => options.isHandoffActive?.() === true
  const relaunchPending = () => options.isRelaunchPending?.() === true

  return {
    get active() {
      return phase !== 'idle'
    },
    get draining() {
      return phase === 'draining'
    },
    assertOperationAllowed(operation: string) {
      if (handoffActive()) {
        throw new Error(`Hermes Desktop handoff is active; ${operation} is blocked.`)
      }

      if (relaunchPending()) {
        throw new Error(`Hermes Desktop restart is already scheduled; ${operation} is blocked.`)
      }

      if (phase !== 'idle') {
        throw new Error(`Hermes Desktop is relaunching; ${operation} is blocked.`)
      }

      if (operationCount > 0) {
        throw new Error(`Another Hermes Desktop lifecycle operation is active; ${operation} is blocked.`)
      }
    },
    assertRelaunchNotActive(operation: string) {
      if (handoffActive()) {
        throw new Error(`Hermes Desktop handoff is active; ${operation} is blocked.`)
      }

      if (phase !== 'idle') {
        throw new Error(`Hermes Desktop is relaunching; ${operation} is blocked.`)
      }
    },
    markRelaunching() {
      if (phase === 'draining') {
        phase = 'relaunching'
      }
    },
    request() {
      if (inFlight) {
        return inFlight
      }

      if (handoffActive()) {
        return Promise.resolve({ ok: false, reason: 'handoff-active' })
      }

      if (phase !== 'idle') {
        return Promise.resolve({ ok: false, reason: 'relaunch-active' })
      }

      if (operationCount > 0) {
        return Promise.resolve({ ok: false, reason: 'operation-active' })
      }

      phase = 'draining'
      const run = Promise.resolve().then(requestRelaunch)

      inFlight = run
        .then(
          result => {
            phase = result.ok ? 'relaunching' : 'idle'

            return result
          },
          error => {
            phase = 'idle'
            throw error
          }
        )
        .finally(() => {
          inFlight = null
        })

      return inFlight
    },
    runOperation<T>(operation: string, callback: (assertActive: () => void) => Promise<T> | T) {
      if (handoffActive()) {
        return Promise.reject(new Error(`Hermes Desktop handoff is active; ${operation} is blocked.`))
      }

      if (relaunchPending()) {
        return Promise.reject(new Error(`Hermes Desktop restart is already scheduled; ${operation} is blocked.`))
      }

      if (phase !== 'idle') {
        return Promise.reject(new Error(`Hermes Desktop is relaunching; ${operation} is blocked.`))
      }

      if (operationCount > 0) {
        return Promise.reject(new Error(`Another Hermes Desktop lifecycle operation is active; ${operation} is blocked.`))
      }

      operationCount += 1

      const assertActive = () => {
        if (handoffActive() || relaunchPending() || phase !== 'idle' || operationCount !== 1) {
          throw new Error(`Hermes Desktop lifecycle lease expired; ${operation} is blocked.`)
        }
      }

      let result: Promise<T> | T

      try {
        result = callback(assertActive)
      } catch (error) {
        operationCount -= 1

        return Promise.reject(error)
      }

      return Promise.resolve(result).finally(() => {
        operationCount -= 1
      })
    }
  }
}

export function createDesktopStartupFence(
  lifecycle: DesktopRelaunchLifecycle,
  label = 'primary backend'
): DesktopStartupFence {
  let generation = 0

  return {
    assertCurrent(candidate: number) {
      lifecycle.assertRelaunchNotActive('backend startup')

      if (candidate !== generation) {
        throw new Error(`Hermes Desktop ${label} startup was superseded.`)
      }
    },
    begin() {
      lifecycle.assertRelaunchNotActive('backend startup')
      generation += 1

      return generation
    },
    invalidate() {
      generation += 1
    },
    isCurrent(candidate: number) {
      return candidate === generation
    }
  }
}

export function ensureDesktopOwnedStartup<K, T, E extends DesktopOwnedStartupEntry<T>>(
  options: DesktopOwnedStartupOptions<K, T, E>
): Promise<T> {
  const existing = options.entries.get(options.key)

  if (existing?.connectionPromise) {
    return existing.connectionPromise
  }

  const entry = options.createEntry()
  const fence = createDesktopStartupFence(options.lifecycle, options.label)
  const generation = fence.begin()
  let invalidated = false

  const owner: DesktopBackendStartupOwner = {
    assertCurrent(assertLease) {
      assertLease?.()
      fence.assertCurrent(generation)

      if (invalidated || options.entries.get(options.key) !== entry) {
        throw new Error(`Hermes Desktop ${options.label} startup was superseded.`)
      }
    },
    invalidate() {
      invalidated = true
      fence.invalidate()
    },
    isCurrent() {
      return !invalidated && fence.isCurrent(generation) && options.entries.get(options.key) === entry
    }
  }

  entry.startupOwner = owner

  const runStartup = async (assertLease: () => void) => {
    await options.beforeStart?.(assertLease)
    owner.assertCurrent(assertLease)
    const result = await options.start(entry, () => owner.assertCurrent(assertLease))
    owner.assertCurrent(assertLease)
    entry.starting = false

    return result
  }

  const startup = options.activeLease
    ? runStartup(options.activeLease)
    : options.lifecycle.runOperation(options.operation, runStartup)

  const connectionPromise = startup.catch(error => {
    invalidateDesktopOwnedStartupEntry(options.entries, options.key, entry)
    throw error
  })

  entry.connectionPromise = connectionPromise
  options.entries.set(options.key, entry)

  return connectionPromise
}

export function invalidateDesktopOwnedStartupEntry<K, T, E extends DesktopOwnedStartupEntry<T>>(
  entries: Map<K, E>,
  key: K,
  entry: E
): boolean {
  if (entries.get(key) !== entry) {
    return false
  }

  entry.startupOwner?.invalidate()
  entries.delete(key)

  return true
}

export async function runOperationWithPostRelease<T>(
  lifecycle: DesktopRelaunchLifecycle,
  operation: string,
  callback: (assertActive: () => void) => Promise<T> | T,
  afterRelease: (result: T) => Promise<void> | void
): Promise<T> {
  const result = await lifecycle.runOperation(operation, callback)

  await afterRelease(result)

  return result
}

export async function requestRelaunchWithRecovery(
  lifecycle: Pick<DesktopRelaunchLifecycle, 'request'>,
  recover: () => Promise<void> | void
): Promise<RelaunchResult> {
  const result = await lifecycle.request()

  if (
    !result.ok &&
    (result.reason === 'relaunch-failed' || result.reason === 'quit-failed-after-relaunch')
  ) {
    await recover()
  }

  return result
}

export function runRelaunchQuitHandoff(options: {
  markRelaunching: () => void
  quit: () => void
  setHandoffActive: (active: boolean) => void
}): void {
  options.markRelaunching()
  options.setHandoffActive(true)

  try {
    options.quit()
  } catch (error) {
    options.setHandoffActive(false)
    throw error
  }
}

function childExitState(child: BackendChild | null): 'abnormal-exit' | 'exited' | 'running' {
  if (child == null || (child.exitCode === 0 && child.signalCode === null)) {
    return 'exited'
  }

  if (child.exitCode !== null || child.signalCode !== null) {
    return 'abnormal-exit'
  }

  return 'running'
}

export function isBackendChildRunning(child: BackendChild | null): boolean {
  return child !== null && child.exitCode === null && child.signalCode === null
}

async function defaultWriteMarker(markerPath: string): Promise<void> {
  await fs.promises.writeFile(
    markerPath,
    JSON.stringify({ pid: process.pid, reason: 'desktop-relaunch', requestedAt: new Date().toISOString() }),
    { encoding: 'utf8', flag: 'wx' }
  )
}

async function defaultRemoveMarker(markerPath: string): Promise<void> {
  await fs.promises.rm(markerPath, { force: true })
}

function waitForNaturalExit(child: BackendChild, timeoutMs: number): Promise<BackendShutdownReason> {
  const initial = childExitState(child)

  if (initial !== 'running') {
    return Promise.resolve(initial)
  }

  return new Promise(resolve => {
    let settled = false

    const finish = (reason: BackendShutdownReason) => {
      if (settled) {
        return
      }

      settled = true
      clearTimeout(timer)
      child.removeListener('exit', onExit)
      resolve(reason)
    }

    const onExit = (code: null | number, signal: null | string) =>
      finish(code === 0 && signal === null ? 'exited' : 'abnormal-exit')

    const timer = setTimeout(() => finish('timeout'), timeoutMs)

    child.once('exit', onExit)

    const raced = childExitState(child)

    if (raced !== 'running') {
      finish(raced)
    }
  })
}

export async function requestBackendShutdown(
  target: BackendShutdownTarget,
  options: ShutdownOptions = {}
): Promise<BackendShutdownResult> {
  const { child, label, markerPath } = target
  const initial = childExitState(child)

  if (initial === 'exited') {
    return { label, ok: true, reason: 'exited' }
  }

  if (initial === 'abnormal-exit') {
    return { label, ok: false, reason: 'abnormal-exit' }
  }

  if (!markerPath) {
    return { label, ok: false, reason: 'unsupported' }
  }

  const writeMarker = options.writeMarker ?? defaultWriteMarker
  const removeMarker = options.removeMarker ?? defaultRemoveMarker
  const timeoutMs = options.timeoutMs ?? DEFAULT_BACKEND_SHUTDOWN_TIMEOUT_MS

  try {
    await writeMarker(markerPath)
  } catch {
    return { label, ok: false, reason: 'marker-write-failed' }
  }

  try {
    const reason = await waitForNaturalExit(child!, timeoutMs)

    return { label, ok: reason === 'exited', reason }
  } finally {
    try {
      await removeMarker(markerPath)
    } catch {
      // Marker cleanup is best-effort after the backend result is known.
    }
  }
}

export async function requestBackendGroupShutdown(
  targets: BackendShutdownTarget[],
  options: ShutdownOptions = {}
): Promise<ShutdownGroupResult> {
  const results = await Promise.all(targets.map(target => requestBackendShutdown(target, options)))

  return { ok: results.every(result => result.ok), results }
}

export function createGracefulRelaunchCoordinator(options: RelaunchCoordinatorOptions): GracefulRelaunchCoordinator {
  let inFlight: Promise<RelaunchResult> | null = null
  let relaunchIssued = false

  const requestRelaunch = function requestRelaunch(): Promise<RelaunchResult> {
    if (inFlight) {
      return inFlight
    }

    const run = async (): Promise<RelaunchResult> => {
      const preflight = options.preflight()

      if (!preflight.ok) {
        return { ok: false, reason: preflight.reason || 'preflight-failed' }
      }

      const shutdownTargets = options.shutdownTargets ?? requestBackendGroupShutdown
      let drained: ShutdownGroupResult

      try {
        drained = await shutdownTargets(options.getTargets())
      } catch {
        return { ok: false, reason: 'backend-drain-failed' }
      }

      if (!drained.ok) {
        return { ok: false, reason: 'backend-drain-failed' }
      }

      try {
        if (!relaunchIssued) {
          options.relaunch()
          relaunchIssued = true
        }

        options.quit()
      } catch {
        return {
          ok: false,
          reason: relaunchIssued ? 'quit-failed-after-relaunch' : 'relaunch-failed'
        }
      }

      return { ok: true, reason: 'relaunching' }
    }

    inFlight = run().finally(() => {
      inFlight = null
    })

    return inFlight
  }

  Object.defineProperty(requestRelaunch, 'relaunchIssued', {
    enumerable: true,
    get: () => relaunchIssued
  })

  return requestRelaunch as GracefulRelaunchCoordinator
}

export { DEFAULT_BACKEND_SHUTDOWN_TIMEOUT_MS }
