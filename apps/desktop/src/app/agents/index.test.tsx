import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'

import { parseDelegationStatus } from '@/store/delegation-progress'

import { DelegationWorkspace } from './index'

afterEach(cleanup)

describe('DelegationWorkspace', () => {
  it('renders split panes, truthful progress, heartbeat, and detail tabs', () => {
    const snapshot = parseDelegationStatus({
      schema_version: 2,
      snapshot_at: 110,
      delegations: [
        {
          delegation_id: 'deleg-1',
          goal: 'Build progress API',
          status: 'running',
          phase: 'waiting_peer',
          total_count: 2,
          finished_count: 1,
          heartbeat_at: 108,
          heartbeat_age_seconds: 2,
          children: [
            {
              task_index: 0,
              goal: 'Backend',
              status: 'completed',
              phase: 'completed',
              heartbeat_at: 105,
              heartbeat_age_seconds: 5
            },
            {
              task_index: 1,
              goal: 'Desktop',
              status: 'running',
              phase: 'tool',
              heartbeat_at: 108,
              heartbeat_age_seconds: 2,
              current_tool: 'patch'
            }
          ]
        }
      ]
    })

    render(<DelegationWorkspace error={null} snapshot={snapshot} tree={[]} unavailable={false} />)

    expect(screen.getByTestId('delegation-progress-upper')).toBeTruthy()
    expect(screen.getByTestId('delegation-progress-lower')).toBeTruthy()
    expect(screen.getByRole('progressbar', { name: 'deleg-1' }).getAttribute('aria-valuenow')).toBe('50')
    expect(screen.queryByText('Build progress API')).toBeNull()
    expect(screen.queryByText('Backend')).toBeNull()
    expect(screen.queryByText('Desktop')).toBeNull()
    expect(screen.getAllByText('1/2 tasks finished')).toHaveLength(2)
    expect(screen.getAllByText(/Heartbeat/).length).toBeGreaterThanOrEqual(2)
    expect(screen.getByRole('tab', { name: 'Progress' })).toBeTruthy()
    expect(screen.getByRole('tab', { name: 'Agent tree' })).toBeTruthy()
    expect(screen.getByRole('tabpanel')).toBeTruthy()
  })

  it('does not label a terminal delegation as live', () => {
    const snapshot = parseDelegationStatus({
      schema_version: 1,
      snapshot_at: 110,
      delegations: [
        {
          delegation_id: 'deleg-done',
          goal: 'Finished task',
          status: 'completed',
          phase: 'completed',
          total_count: 1,
          finished_count: 1,
          completed_count: 1,
          children: [{ task_index: 0, goal: 'Done child', status: 'completed', phase: 'completed' }]
        }
      ]
    })

    render(<DelegationWorkspace error={null} snapshot={snapshot} tree={[]} unavailable={false} />)

    expect(screen.queryByText('Live')).toBeNull()
    expect(screen.getAllByText('Done').length).toBeGreaterThanOrEqual(1)
  })

  it('labels interrupted and unknown states distinctly from failed', () => {
    const snapshot = parseDelegationStatus({
      schema_version: 1,
      delegations: [
        {
          delegation_id: 'deleg-interrupted',
          status: 'interrupted',
          total_count: 2,
          finished_count: 2,
          failed_count: 2,
          children: [
            { task_index: 0, phase: 'tool' },
            { task_index: 1, status: 'interrupted', phase: 'tool' }
          ]
        },
        {
          delegation_id: 'deleg-unknown',
          status: 'unknown',
          total_count: 1,
          finished_count: 1,
          failed_count: 1,
          children: []
        }
      ]
    })

    render(<DelegationWorkspace error={null} snapshot={snapshot} tree={[]} unavailable={false} />)

    expect(screen.getAllByText('Interrupted').length).toBeGreaterThanOrEqual(2)
    expect(screen.getAllByText('Unknown').length).toBeGreaterThanOrEqual(2)
    expect(screen.queryByText('Tool')).toBeNull()
  })
})
