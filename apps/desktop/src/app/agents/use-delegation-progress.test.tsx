import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { $gateway } from '@/store/gateway'
import { $activeGatewayProfile } from '@/store/profile'
import { $activeSessionId, $gatewayState } from '@/store/session'

import { useDelegationProgress } from './use-delegation-progress'

const requestGateway = vi.hoisted(() => vi.fn())

vi.mock('@/app/gateway/hooks/use-gateway-request', () => ({
  useGatewayRequest: () => ({ requestGateway })
}))

function Probe() {
  const { error, snapshot, unavailable } = useDelegationProgress()

  return (
    <div>
      <span data-testid="unavailable">{String(unavailable)}</span>
      <span data-testid="delegation-id">{snapshot?.delegations[0]?.id ?? ''}</span>
      <span data-testid="error">{error ?? ''}</span>
    </div>
  )
}

beforeEach(() => {
  requestGateway.mockReset()
  Object.defineProperty(document, 'hidden', { configurable: true, value: false })
  $gateway.set(null)
  $activeGatewayProfile.set('default')
  $activeSessionId.set('owner-ui')
  $gatewayState.set('open')
})

afterEach(cleanup)

describe('useDelegationProgress', () => {
  it('polls the dedicated progress RPC and parses the snapshot', async () => {
    requestGateway.mockResolvedValue({
      schema_version: 1,
      snapshot_at: 123,
      delegations: [{ delegation_id: 'deleg-1', total_count: 1, finished_count: 0 }]
    })

    render(<Probe />)

    await waitFor(() => expect(screen.getByTestId('delegation-id').textContent).toBe('deleg-1'))
    expect(requestGateway).toHaveBeenCalledWith(
      'delegation.progress',
      { session_id: 'owner-ui' },
      5_000,
      expect.any(AbortSignal)
    )
    expect(screen.getByTestId('unavailable').textContent).toBe('false')
  })

  it('stops retrying when an older backend does not expose the RPC', async () => {
    requestGateway.mockRejectedValue(new Error('unknown method: delegation.progress'))

    render(<Probe />)

    await waitFor(() => expect(screen.getByTestId('unavailable').textContent).toBe('true'))
    expect(requestGateway).toHaveBeenCalledTimes(1)
  })

  it('re-probes progress support after an open-to-open active profile change', async () => {
    requestGateway
      .mockRejectedValueOnce(new Error('unknown method: delegation.progress'))
      .mockResolvedValue({
        schema_version: 1,
        delegations: [{ delegation_id: 'deleg-new', status: 'running', total_count: 1, finished_count: 0 }]
      })

    render(<Probe />)
    await waitFor(() => expect(screen.getByTestId('unavailable').textContent).toBe('true'))

    $activeGatewayProfile.set('work')

    await waitFor(() => expect(screen.getByTestId('delegation-id').textContent).toBe('deleg-new'))
    expect(screen.getByTestId('unavailable').textContent).toBe('false')
    expect(requestGateway).toHaveBeenLastCalledWith(
      'delegation.progress',
      { session_id: 'owner-ui' },
      5_000,
      expect.any(AbortSignal)
    )
  })

  it('re-probes progress support after a same-profile gateway instance change', async () => {
    requestGateway
      .mockRejectedValueOnce(new Error('unknown method: delegation.progress'))
      .mockResolvedValue({
        schema_version: 1,
        delegations: [{ delegation_id: 'deleg-replaced', status: 'running', total_count: 1, finished_count: 0 }]
      })

    render(<Probe />)
    await waitFor(() => expect(screen.getByTestId('unavailable').textContent).toBe('true'))

    $gateway.set({} as never)

    await waitFor(() => expect(screen.getByTestId('delegation-id').textContent).toBe('deleg-replaced'))
    expect(screen.getByTestId('unavailable').textContent).toBe('false')
  })

  it('invalidates an old live snapshot after a transient request failure', async () => {
    requestGateway
      .mockResolvedValueOnce({
        schema_version: 1,
        snapshot_at: 123,
        delegations: [{ delegation_id: 'deleg-1', status: 'running', total_count: 1, finished_count: 0 }]
      })
      .mockRejectedValue(new Error('temporary disconnect'))

    render(<Probe />)
    await waitFor(() => expect(screen.getByTestId('delegation-id').textContent).toBe('deleg-1'))
    await new Promise(resolve => window.setTimeout(resolve, 1_650))
    await waitFor(() => expect(screen.getByTestId('error').textContent).toBe('transient'))
    expect(screen.getByTestId('delegation-id').textContent).toBe('')
  })

  it('does not poll while the document is hidden and resumes when visible', async () => {
    Object.defineProperty(document, 'hidden', { configurable: true, value: true })
    requestGateway.mockResolvedValue({ schema_version: 1, delegations: [] })

    render(<Probe />)
    await new Promise(resolve => window.setTimeout(resolve, 25))
    expect(requestGateway).not.toHaveBeenCalled()

    Object.defineProperty(document, 'hidden', { configurable: true, value: false })
    fireEvent(document, new Event('visibilitychange'))
    await waitFor(() => expect(requestGateway).toHaveBeenCalledTimes(1))
  })

  it('does not poll while the Gateway is disconnected', async () => {
    $gatewayState.set('closed')

    render(<Probe />)

    await waitFor(() => expect(screen.getByTestId('error').textContent).toBe('disconnected'))
    expect(requestGateway).not.toHaveBeenCalled()
  })
})
