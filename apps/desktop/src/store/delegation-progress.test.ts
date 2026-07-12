import { describe, expect, it } from 'vitest'

import { parseDelegationStatus } from './delegation-progress'

describe('delegation progress payload', () => {
  it('recomputes completion percent from finished and total counts', () => {
    const parsed = parseDelegationStatus({
      schema_version: 2,
      process_instance_id: 'proc-1',
      process_local: true,
      snapshot_at: 123,
      delegations: [
        {
          delegation_id: 'deleg-1',
          goal: 'build feature',
          status: 'running',
          phase: 'waiting_peer',
          total_count: 4,
          finished_count: 1,
          progress_percent: 99,
          heartbeat_at: 120,
          heartbeat_age_seconds: 3,
          children: [
            { task_index: 0, goal: 'one', status: 'completed', phase: 'completed', heartbeat_at: 119 },
            {
              task_index: 1,
              goal: 'two',
              status: 'running',
              phase: 'tool',
              heartbeat_at: 120,
              current_tool: 'terminal'
            }
          ]
        }
      ]
    })

    expect(parsed.delegations).toHaveLength(1)
    expect(parsed.processInstanceId).toBe('proc-1')
    expect(parsed.processLocal).toBe(true)
    expect(parsed.delegations[0]?.progressPercent).toBe(25)
    expect(parsed.delegations[0]).not.toHaveProperty('goal')
    expect(parsed.delegations[0]?.children[0]).not.toHaveProperty('goal')
    expect(parsed.delegations[0]?.children[1]?.currentTool).toBe('terminal')
  })

  it('drops malformed rows and clamps impossible counts', () => {
    const parsed = parseDelegationStatus({
      schema_version: 2,
      delegations: [
        null,
        {},
        {
          delegation_id: 'ok',
          total_count: 5,
          finished_count: 5,
          completed_count: 5,
          failed_count: 5,
          children: [
            { task_index: 0, phase: 'tool' },
            { task_index: 1, status: 'not-a-status', phase: 'tool' },
            { task_index: 2, status: 'interrupted', phase: 'tool' }
          ]
        }
      ]
    })

    expect(parsed.delegations).toHaveLength(1)
    expect(parsed.delegations[0]).toMatchObject({
      id: 'ok',
      status: 'unknown',
      phase: 'unknown',
      totalCount: 5,
      finishedCount: 5,
      completedCount: 5,
      failedCount: 0,
      runningCount: 0,
      progressPercent: 100,
      children: [
        { taskIndex: 0, status: 'unknown', phase: 'unknown' },
        { taskIndex: 1, status: 'unknown', phase: 'unknown' },
        { taskIndex: 2, status: 'interrupted', phase: 'interrupted' }
      ]
    })
  })
})
