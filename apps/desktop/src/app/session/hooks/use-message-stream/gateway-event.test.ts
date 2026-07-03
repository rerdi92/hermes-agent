import { describe, expect, it } from 'vitest'

import type { GatewayEventPayload } from '@/lib/chat-messages'
import type { SessionInfo } from '@/types/hermes'

import { clarifyRequestFromPayload } from './gateway-event'

const session = ({ id, ...row }: Partial<SessionInfo> & Pick<SessionInfo, 'id'>): SessionInfo =>
  ({
    ended_at: null,
    id,
    input_tokens: 0,
    is_active: false,
    last_active: 0,
    message_count: 0,
    model: null,
    output_tokens: 0,
    preview: null,
    source: null,
    started_at: 0,
    title: null,
    tool_call_count: 0,
    ...row
  }) as SessionInfo

describe('clarifyRequestFromPayload', () => {
  it('preserves clarify metadata from gateway payload and payload profile', () => {
    const request = clarifyRequestFromPayload(
      {
        request_id: 'req-123',
        question: 'Which sources?',
        choices: ['Past sessions', 'Local reports', 7, 'Web sources'],
        multi_select: true,
        min_selections: 2,
        max_selections: 3,
        allow_other: false,
        profile: 'research'
      } as GatewayEventPayload,
      'runtime-1',
      [session({ id: 'runtime-1', profile: 'default' })]
    )

    expect(request).toEqual({
      requestId: 'req-123',
      question: 'Which sources?',
      choices: ['Past sessions', 'Local reports', 'Web sources'],
      sessionId: 'runtime-1',
      multiSelect: true,
      minSelections: 2,
      maxSelections: 3,
      allowOther: false,
      profile: 'research'
    })
  })

  it('falls back to the owning session profile when payload profile is absent', () => {
    const request = clarifyRequestFromPayload(
      {
        request_id: 'req-456',
        question: 'Continue?',
        choices: ['Yes', 'No']
      } as GatewayEventPayload,
      'runtime-child',
      [session({ id: 'runtime-root', _lineage_root_id: 'runtime-child', profile: 'ops' })]
    )

    expect(request?.profile).toBe('ops')
    expect(request?.allowOther).toBe(true)
    expect(request?.multiSelect).toBe(false)
    expect(request?.minSelections).toBeNull()
    expect(request?.maxSelections).toBeNull()
  })

  it('returns null for malformed clarify requests', () => {
    expect(clarifyRequestFromPayload({ request_id: 'req-only' } as GatewayEventPayload, 'runtime-1', [])).toBeNull()
    expect(clarifyRequestFromPayload({ question: 'Missing id' } as GatewayEventPayload, 'runtime-1', [])).toBeNull()
  })
})
