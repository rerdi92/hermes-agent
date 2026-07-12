import { useStore } from '@nanostores/react'
import { useEffect, useState } from 'react'

import { useGatewayRequest } from '@/app/gateway/hooks/use-gateway-request'
import { isMissingRpcMethod } from '@/lib/gateway-rpc'
import {
  type DelegationStatusSnapshot,
  parseDelegationStatus
} from '@/store/delegation-progress'
import { $gateway } from '@/store/gateway'
import { $activeGatewayProfile } from '@/store/profile'
import { $activeSessionId, $gatewayState } from '@/store/session'

const ACTIVE_POLL_INTERVAL_MS = 1_500
const IDLE_POLL_INTERVAL_MS = 5_000

export type DelegationProgressError = 'disconnected' | 'transient' | null

export function useDelegationProgress() {
  const { requestGateway } = useGatewayRequest()
  const activeGateway = useStore($gateway)
  const activeGatewayProfile = useStore($activeGatewayProfile)
  const activeSessionId = useStore($activeSessionId)
  const gatewayState = useStore($gatewayState)
  const [snapshot, setSnapshot] = useState<DelegationStatusSnapshot | null>(null)
  const [unavailable, setUnavailable] = useState(false)
  const [error, setError] = useState<DelegationProgressError>(null)

  useEffect(() => {
    setSnapshot(null)
    setError(null)
    setUnavailable(false)

    if (!activeSessionId) {
      return
    }

    if (gatewayState !== 'open') {
      setError('disconnected')

      return
    }

    let disposed = false
    let inFlight = false
    let apiMissing = false
    let timer: number | null = null
    let hasRunningDelegation = true
    const abort = new AbortController()

    const clearTimer = () => {
      if (timer !== null) {
        window.clearTimeout(timer)
        timer = null
      }
    }

    const schedule = () => {
      clearTimer()

      if (disposed || apiMissing || document.hidden) {
        return
      }

      timer = window.setTimeout(
        () => void refresh(),
        hasRunningDelegation ? ACTIVE_POLL_INTERVAL_MS : IDLE_POLL_INTERVAL_MS
      )
    }

    const refresh = async () => {
      if (disposed || inFlight || apiMissing || document.hidden) {
        return
      }

      inFlight = true

      try {
        const payload = await requestGateway<unknown>(
          'delegation.progress',
          { session_id: activeSessionId },
          5_000,
          abort.signal
        )

        if (!disposed) {
          const next = parseDelegationStatus(payload)
          hasRunningDelegation = next.delegations.some(item => item.status === 'running')
          setSnapshot(next)
          setError(null)
        }
      } catch (requestError) {
        if (!disposed && isMissingRpcMethod(requestError)) {
          apiMissing = true
          setSnapshot(null)
          setUnavailable(true)
        } else if (!disposed) {
          setSnapshot(null)
          setError('transient')
        }
      } finally {
        inFlight = false
        schedule()
      }
    }

    const onVisibilityChange = () => {
      if (document.hidden) {
        clearTimer()
      } else {
        void refresh()
      }
    }

    document.addEventListener('visibilitychange', onVisibilityChange)
    void refresh()

    return () => {
      disposed = true
      clearTimer()
      abort.abort()
      document.removeEventListener('visibilitychange', onVisibilityChange)
    }
  }, [activeGateway, activeGatewayProfile, activeSessionId, gatewayState, requestGateway])

  return { error, snapshot, unavailable }
}
