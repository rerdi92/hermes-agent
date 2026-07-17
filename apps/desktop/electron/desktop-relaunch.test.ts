import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'

import { test } from 'vitest'

import {
  type BackendShutdownTarget,
  createDesktopRelaunchLifecycle,
  createDesktopStartupFence,
  createGracefulRelaunchCoordinator,
  decideDesktopRelaunchPreflight,
  type DesktopBackendStartupOwner,
  ensureDesktopOwnedStartup,
  invalidateDesktopOwnedStartupEntry,
  isBackendChildRunning,
  requestBackendShutdown,
  requestRelaunchWithRecovery,
  runOperationWithPostRelease,
  runRelaunchQuitHandoff,
} from './desktop-relaunch'

class FakeChild extends EventEmitter {
  exitCode: null | number = null
  killed = false
  signalCode: null | string = null

  kill(): never {
    this.killed = true
    throw new Error('graceful relaunch must never kill the backend')
  }
}

function target(child: FakeChild, markerPath = 'C:/Temp/hermes-desktop-shutdown.json'): BackendShutdownTarget {
  return { child, label: 'primary', markerPath }
}

test('preflight blocks unresolved local startup but permits an established remote connection', () => {
  const base = {
    activeTerminalCount: 0,
    bootstrapActive: false,
    handoffActive: false,
    hasConnectionPromise: true,
    hasLocalBackendChild: false,
    unresolvedPoolCount: 0,
    updateInFlight: false
  }

  assert.deepEqual(decideDesktopRelaunchPreflight({ ...base, primaryConnectionMode: null }), {
    ok: false,
    reason: 'backend-starting'
  })
  assert.deepEqual(decideDesktopRelaunchPreflight({ ...base, primaryConnectionMode: 'remote' }), { ok: true })
})

test('preflight blocks handoff, unresolved pool startup, and child-present unresolved primary startup', () => {
  const base = {
    activeTerminalCount: 0,
    bootstrapActive: false,
    handoffActive: false,
    hasConnectionPromise: false,
    hasLocalBackendChild: false,
    primaryConnectionMode: null,
    unresolvedPoolCount: 0,
    updateInFlight: false
  }

  assert.deepEqual(decideDesktopRelaunchPreflight({ ...base, handoffActive: true }), {
    ok: false,
    reason: 'handoff-active'
  })
  assert.deepEqual(decideDesktopRelaunchPreflight({ ...base, unresolvedPoolCount: 1 }), {
    ok: false,
    reason: 'pool-starting'
  })
  assert.deepEqual(
    decideDesktopRelaunchPreflight({
      ...base,
      hasConnectionPromise: true,
      hasLocalBackendChild: true
    }),
    { ok: false, reason: 'backend-starting' }
  )
})

test('lifecycle gate fences competing teardown and PTY operations throughout a pending drain', async () => {
  let coordinateCalls = 0
  let forceTeardownCalls = 0
  let releaseDrain: (() => void) | undefined

  const drain = new Promise<void>(resolve => {
    releaseDrain = resolve
  })

  const lifecycle = createDesktopRelaunchLifecycle(async () => {
    coordinateCalls += 1
    await drain

    return { ok: true, reason: 'relaunching' }
  })

  const first = lifecycle.request()
  const second = lifecycle.request()
  assert.equal(first, second)
  assert.equal(lifecycle.active, true)

  for (const operation of ['backend startup', 'update', 'uninstall', 'connection apply', 'profile switch', 'terminal start']) {
    assert.throws(() => lifecycle.assertOperationAllowed(operation), /relaunching/)
  }

  if (!lifecycle.active) {
    forceTeardownCalls += 1
  }

  assert.equal(forceTeardownCalls, 0)
  releaseDrain?.()
  assert.deepEqual(await first, { ok: true, reason: 'relaunching' })
  assert.equal(coordinateCalls, 1)
  assert.equal(lifecycle.active, true)
})

test('lifecycle operation leases block relaunch in both race directions', async () => {
  let coordinateCalls = 0
  let releaseDrain: (() => void) | undefined

  const drain = new Promise<void>(resolve => {
    releaseDrain = resolve
  })

  const lifecycle = createDesktopRelaunchLifecycle(async () => {
    coordinateCalls += 1
    await drain

    return { ok: true, reason: 'relaunching' }
  })

  for (const operation of ['pool idle reaper', 'update', 'uninstall', 'connection apply', 'profile delete']) {
    let releaseMutation: (() => void) | undefined

    const mutation = new Promise<void>(resolve => {
      releaseMutation = resolve
    })

    const competingOperation = lifecycle.runOperation(operation, async () => mutation)

    assert.deepEqual(await lifecycle.request(), { ok: false, reason: 'operation-active' })
    releaseMutation?.()
    await competingOperation
  }

  assert.equal(coordinateCalls, 0)

  const relaunch = lifecycle.request()
  assert.equal(lifecycle.draining, true)
  await assert.rejects(lifecycle.runOperation('update', async () => undefined), /relaunching/)
  releaseDrain?.()
  assert.deepEqual(await relaunch, { ok: true, reason: 'relaunching' })
  assert.equal(coordinateCalls, 1)
  assert.equal(lifecycle.draining, false)
})

test('durable handoff dwell blocks every new lifecycle operation after the owner lease returns', async () => {
  let handoffActive = false

  const lifecycle = createDesktopRelaunchLifecycle(
    async () => ({ ok: true, reason: 'relaunching' }),
    { isHandoffActive: () => handoffActive }
  )

  await lifecycle.runOperation('update', async () => {
    handoffActive = true
  })

  for (const operation of [
    'pool backend startup',
    'primary backend startup',
    'terminal start',
    'connection apply',
    'profile switch',
    'profile delete',
    'update',
    'uninstall'
  ]) {
    assert.throws(() => lifecycle.assertOperationAllowed(operation), /handoff/)
    await assert.rejects(lifecycle.runOperation(operation, async () => undefined), /handoff/)
  }

  assert.deepEqual(await lifecycle.request(), { ok: false, reason: 'handoff-active' })
})

test('owned pool startup shares one promise, holds the lifecycle lease, and rejects a stale continuation', async () => {
  type Entry = {
    connectionPromise: Promise<string> | null
    starting: boolean
    startupOwner: DesktopBackendStartupOwner | null
  }

  const entries = new Map<string, Entry>()
  const lifecycle = createDesktopRelaunchLifecycle(async () => ({ ok: true, reason: 'relaunching' }))
  let releaseStartup: (() => void) | undefined
  let spawnCalls = 0

  const startupWait = new Promise<void>(resolve => {
    releaseStartup = resolve
  })

  const options = {
    createEntry: (): Entry => ({ connectionPromise: null, starting: true, startupOwner: null }),
    entries,
    key: 'researcher',
    label: 'profile:researcher',
    lifecycle,
    operation: 'pool backend startup',
    start: async (_entry: Entry, assertCurrent: () => void) => {
      await startupWait
      assertCurrent()
      spawnCalls += 1

      return 'connected'
    }
  }

  const first = ensureDesktopOwnedStartup(options)
  const second = ensureDesktopOwnedStartup(options)
  assert.equal(first, second)
  await assert.rejects(lifecycle.runOperation('connection apply', async () => undefined), /operation is active/)
  assert.deepEqual(await lifecycle.request(), { ok: false, reason: 'operation-active' })

  const staleEntry = entries.get('researcher')
  assert.ok(staleEntry)
  assert.equal(invalidateDesktopOwnedStartupEntry(entries, 'researcher', staleEntry), true)
  const replacement: Entry = { connectionPromise: Promise.resolve('replacement'), starting: false, startupOwner: null }
  entries.set('researcher', replacement)
  releaseStartup?.()

  await assert.rejects(first, /superseded/)
  assert.equal(spawnCalls, 0)
  assert.equal(entries.get('researcher'), replacement)
  assert.equal(invalidateDesktopOwnedStartupEntry(entries, 'researcher', staleEntry), false)
  assert.equal(entries.get('researcher'), replacement)
})

test('primary startup fence rejects a stale deferred continuation before state commit or spawn', async () => {
  const lifecycle = createDesktopRelaunchLifecycle(async () => ({ ok: true, reason: 'relaunching' }))
  const startupFence = createDesktopStartupFence(lifecycle)
  const generation = startupFence.begin()
  let releaseDeferred: (() => void) | undefined
  let spawned = false

  const deferred = new Promise<void>(resolve => {
    releaseDeferred = resolve
  })

  const continuation = (async () => {
    await deferred
    startupFence.assertCurrent(generation)
    spawned = true
  })()

  await lifecycle.runOperation('connection apply', async () => {
    startupFence.invalidate()
    releaseDeferred?.()
    await assert.rejects(continuation, /superseded|operation is active/)
  })

  assert.equal(spawned, false)
  assert.throws(() => startupFence.assertCurrent(generation), /superseded/)
  const replacementGeneration = startupFence.begin()
  assert.doesNotThrow(() => startupFence.assertCurrent(replacementGeneration))
})

test('primary startup lease blocks update and relaunch across deferred startup work', async () => {
  let coordinateCalls = 0
  let releaseStartup: (() => void) | undefined

  const startupWait = new Promise<void>(resolve => {
    releaseStartup = resolve
  })

  const lifecycle = createDesktopRelaunchLifecycle(async () => {
    coordinateCalls += 1

    return { ok: true, reason: 'relaunching' }
  })

  let startupStarted = false

  const startup = lifecycle.runOperation('primary backend startup', async assertActive => {
    startupStarted = true
    await startupWait
    assertActive()
  })

  assert.equal(startupStarted, true)
  await assert.rejects(lifecycle.runOperation('update', async () => undefined), /operation is active/)
  assert.deepEqual(await lifecycle.request(), { ok: false, reason: 'operation-active' })
  assert.equal(coordinateCalls, 0)
  releaseStartup?.()
  await startup
  assert.doesNotThrow(() => lifecycle.assertOperationAllowed('update'))
})

test('profile delete lease remains active through deferred request completion', async () => {
  let releaseRequest: (() => void) | undefined

  const requestWait = new Promise<void>(resolve => {
    releaseRequest = resolve
  })

  const lifecycle = createDesktopRelaunchLifecycle(async () => ({ ok: true, reason: 'relaunching' }))

  const deletion = lifecycle.runOperation('profile delete', async assertActive => {
    await requestWait
    assertActive()
  })

  assert.deepEqual(await lifecycle.request(), { ok: false, reason: 'operation-active' })
  releaseRequest?.()
  await deletion
  assert.doesNotThrow(() => lifecycle.assertOperationAllowed('backend startup'))
})

test('failed update recovery starts only after its lifecycle lease is released', async () => {
  const lifecycle = createDesktopRelaunchLifecycle(async () => ({ ok: true, reason: 'relaunching' }))
  let restarted = false

  const result = await runOperationWithPostRelease(
    lifecycle,
    'update',
    async () => ({ restartBackendAfterLease: true }),
    updateResult => {
      if (updateResult.restartBackendAfterLease) {
        lifecycle.assertOperationAllowed('backend startup')
        restarted = true
      }
    }
  )

  assert.equal(result.restartBackendAfterLease, true)
  assert.equal(restarted, true)
})

test('lifecycle gate releases after an unexpected coordinator rejection', async () => {
  const lifecycle = createDesktopRelaunchLifecycle(async () => {
    throw new Error('unexpected drain failure')
  })

  await assert.rejects(lifecycle.request(), /unexpected drain failure/)
  assert.equal(lifecycle.active, false)
  assert.doesNotThrow(() => lifecycle.assertOperationAllowed('backend startup'))
})

test('backend running guard rejects exited children before any kill fallback can run', () => {
  const child = new FakeChild()
  assert.equal(isBackendChildRunning(child), true)

  child.exitCode = 0
  assert.equal(isBackendChildRunning(child), false)
})

test('requestBackendShutdown writes the owned marker and waits for natural exit', async () => {
  const child = new FakeChild()
  const writes: string[] = []
  const removals: string[] = []

  const result = await requestBackendShutdown(target(child), {
    removeMarker: async markerPath => {
      removals.push(markerPath)
    },
    timeoutMs: 100,
    writeMarker: async markerPath => {
      writes.push(markerPath)
      setTimeout(() => {
        child.exitCode = 0
        child.emit('exit', 0, null)
      }, 0)
    }
  })

  assert.deepEqual(result, { label: 'primary', ok: true, reason: 'exited' })
  assert.deepEqual(writes, ['C:/Temp/hermes-desktop-shutdown.json'])
  assert.deepEqual(removals, ['C:/Temp/hermes-desktop-shutdown.json'])
  assert.equal(child.killed, false)
})

test('requestBackendShutdown fails closed when the backend lacks a marker seam', async () => {
  const child = new FakeChild()
  let wrote = false

  const result = await requestBackendShutdown(target(child, ''), {
    timeoutMs: 10,
    writeMarker: async () => {
      wrote = true
    }
  })

  assert.deepEqual(result, { label: 'primary', ok: false, reason: 'unsupported' })
  assert.equal(wrote, false)
  assert.equal(child.killed, false)
})

test('requestBackendShutdown times out without killing the backend', async () => {
  const child = new FakeChild()

  const result = await requestBackendShutdown(target(child), {
    removeMarker: async () => undefined,
    timeoutMs: 5,
    writeMarker: async () => undefined
  })

  assert.deepEqual(result, { label: 'primary', ok: false, reason: 'timeout' })
  assert.equal(child.killed, false)
})

test('requestBackendShutdown rejects an abnormal child exit', async () => {
  const child = new FakeChild()

  const result = await requestBackendShutdown(target(child), {
    removeMarker: async () => undefined,
    timeoutMs: 100,
    writeMarker: async () => {
      setTimeout(() => {
        child.exitCode = 1
        child.emit('exit', 1, null)
      }, 0)
    }
  })

  assert.deepEqual(result, { label: 'primary', ok: false, reason: 'abnormal-exit' })
  assert.equal(child.killed, false)
})

test('coordinator deduplicates concurrent requests and relaunches only after every backend exits', async () => {
  const child = new FakeChild()
  let shutdownCalls = 0
  let relaunchCalls = 0
  let quitCalls = 0
  let releaseShutdown: (() => void) | undefined

  const shutdownGate = new Promise<void>(resolve => {
    releaseShutdown = resolve
  })

  const requestRelaunch = createGracefulRelaunchCoordinator({
    getTargets: () => [target(child)],
    preflight: () => ({ ok: true as const }),
    quit: () => {
      quitCalls += 1
    },
    relaunch: () => {
      relaunchCalls += 1
    },
    shutdownTargets: async () => {
      shutdownCalls += 1
      await shutdownGate

      return { ok: true as const, results: [{ label: 'primary', ok: true as const, reason: 'exited' as const }] }
    }
  })

  const first = requestRelaunch()
  const second = requestRelaunch()
  assert.equal(first, second)
  releaseShutdown?.()

  assert.deepEqual(await first, { ok: true, reason: 'relaunching' })
  assert.equal(shutdownCalls, 1)
  assert.equal(relaunchCalls, 1)
  assert.equal(quitCalls, 1)
})

test('quit failure recovers and retries without issuing a duplicate relaunch', async () => {
  let failQuit = true
  let handoffActive = false
  let recoveries = 0
  let relaunchCalls = 0
  let lifecycle: ReturnType<typeof createDesktopRelaunchLifecycle>

  const coordinate = createGracefulRelaunchCoordinator({
    getTargets: () => [],
    preflight: () => ({ ok: true as const }),
    quit: () =>
      runRelaunchQuitHandoff({
        markRelaunching: () => lifecycle.markRelaunching(),
        quit: () => {
          if (failQuit) {
            failQuit = false
            throw new Error('synthetic quit failure')
          }
        },
        setHandoffActive: active => {
          handoffActive = active
        }
      }),
    relaunch: () => {
      relaunchCalls += 1
    },
    shutdownTargets: async () => ({ ok: true as const, results: [] })
  })

  lifecycle = createDesktopRelaunchLifecycle(coordinate, {
    isHandoffActive: () => handoffActive,
    isRelaunchPending: () => coordinate.relaunchIssued
  })

  const requestWithRecovery = () =>
    requestRelaunchWithRecovery(lifecycle, async assertActive => {
      assertActive()
      recoveries += 1
    })

  assert.deepEqual(await requestWithRecovery(), { ok: false, reason: 'quit-failed-after-relaunch' })
  assert.equal(handoffActive, false)
  assert.equal(lifecycle.active, false)
  assert.equal(coordinate.relaunchIssued, true)
  assert.equal(recoveries, 1)
  assert.throws(() => lifecycle.assertOperationAllowed('update'), /restart is already scheduled/)

  assert.deepEqual(await requestWithRecovery(), { ok: true, reason: 'relaunching' })
  assert.equal(handoffActive, true)
  assert.equal(lifecycle.active, true)
  assert.equal(recoveries, 1)
  assert.equal(relaunchCalls, 1)
})

test('relaunch publication failure recovers without a pending latch or quit', async () => {
  let quitCalls = 0
  let recoveries = 0

  const coordinate = createGracefulRelaunchCoordinator({
    getTargets: () => [],
    preflight: () => ({ ok: true as const }),
    quit: () => {
      quitCalls += 1
    },
    relaunch: () => {
      throw new Error('synthetic relaunch failure')
    },
    shutdownTargets: async () => ({ ok: true as const, results: [] })
  })

  const lifecycle = createDesktopRelaunchLifecycle(coordinate, {
    isRelaunchPending: () => coordinate.relaunchIssued
  })

  const result = await requestRelaunchWithRecovery(lifecycle, assertActive => {
    assertActive()
    recoveries += 1
  })

  assert.deepEqual(result, { ok: false, reason: 'relaunch-failed' })
  assert.equal(coordinate.relaunchIssued, false)
  assert.equal(quitCalls, 0)
  assert.equal(recoveries, 1)
  assert.equal(lifecycle.active, false)
  assert.doesNotThrow(() => lifecycle.assertOperationAllowed('update'))
})

test('relaunch publication failure recovery holds a real lease against competing operations', async () => {
  let releaseRecovery: (() => void) | undefined
  let signalRecoveryStarted: (() => void) | undefined

  const recoveryGate = new Promise<void>(resolve => {
    releaseRecovery = resolve
  })
  const recoveryStarted = new Promise<void>(resolve => {
    signalRecoveryStarted = resolve
  })
  const coordinate = createGracefulRelaunchCoordinator({
    getTargets: () => [],
    preflight: () => ({ ok: true as const }),
    quit: () => undefined,
    relaunch: () => {
      throw new Error('synthetic relaunch failure')
    },
    shutdownTargets: async () => ({ ok: true as const, results: [] })
  })
  const lifecycle = createDesktopRelaunchLifecycle(coordinate, {
    isRelaunchPending: () => coordinate.relaunchIssued
  })

  const request = requestRelaunchWithRecovery(lifecycle, async assertActive => {
    signalRecoveryStarted?.()
    await recoveryGate
    assertActive()
  })
  await recoveryStarted

  let competingError: unknown
  try {
    await lifecycle.runOperation('update', async () => undefined)
  } catch (error) {
    competingError = error
  } finally {
    releaseRecovery?.()
  }

  assert.match(String(competingError), /operation is active/)
  assert.deepEqual(await request, { ok: false, reason: 'relaunch-failed' })
  assert.doesNotThrow(() => lifecycle.assertOperationAllowed('update'))
})

test('coordinator keeps the app open when preflight or backend drain fails', async () => {
  let shutdownCalls = 0
  let relaunchCalls = 0
  let quitCalls = 0

  const blocked = createGracefulRelaunchCoordinator({
    getTargets: () => [],
    preflight: () => ({ ok: false as const, reason: 'active-terminals' }),
    quit: () => {
      quitCalls += 1
    },
    relaunch: () => {
      relaunchCalls += 1
    },
    shutdownTargets: async () => {
      shutdownCalls += 1

      return { ok: true as const, results: [] }
    }
  })

  assert.deepEqual(await blocked(), { ok: false, reason: 'active-terminals' })
  assert.equal(shutdownCalls, 0)

  const failedDrain = createGracefulRelaunchCoordinator({
    getTargets: () => [],
    preflight: () => ({ ok: true as const }),
    quit: () => {
      quitCalls += 1
    },
    relaunch: () => {
      relaunchCalls += 1
    },
    shutdownTargets: async () => ({
      ok: false as const,
      results: [{ label: 'primary', ok: false as const, reason: 'timeout' as const }]
    })
  })

  assert.deepEqual(await failedDrain(), { ok: false, reason: 'backend-drain-failed' })

  const rejectedDrain = createGracefulRelaunchCoordinator({
    getTargets: () => [],
    preflight: () => ({ ok: true as const }),
    quit: () => {
      quitCalls += 1
    },
    relaunch: () => {
      relaunchCalls += 1
    },
    shutdownTargets: async () => {
      throw new Error('unexpected drain failure')
    }
  })

  assert.deepEqual(await rejectedDrain(), { ok: false, reason: 'backend-drain-failed' })
  assert.equal(relaunchCalls, 0)
  assert.equal(quitCalls, 0)
})
