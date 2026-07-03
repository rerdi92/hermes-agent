import type { GatewayEvent } from '@hermes/shared'
import { afterEach, describe, expect, it } from 'vitest'

import { getGatewayForProfile, setPrimaryGateway, stampSecondaryEventProfile } from './gateway'

const gateway = (label: string) => ({ label }) as never

afterEach(() => {
  setPrimaryGateway(null)
})

describe('stampSecondaryEventProfile', () => {
  it('adds owner profile to object payload while preserving existing fields', () => {
    const event = {
      type: 'clarify.request',
      payload: {
        request_id: 'req-1',
        question: 'Choose?',
        choices: ['A', 'B'],
        multi_select: true
      }
    } as unknown as GatewayEvent

    expect(stampSecondaryEventProfile(event, 'research')).toEqual({
      type: 'clarify.request',
      payload: {
        request_id: 'req-1',
        question: 'Choose?',
        choices: ['A', 'B'],
        multi_select: true,
        profile: 'research'
      }
    })
  })

  it('leaves non-object payloads unchanged', () => {
    const stringEvent = { type: 'message.delta', payload: 'text' } as unknown as GatewayEvent
    const arrayEvent = { type: 'message.delta', payload: ['text'] } as unknown as GatewayEvent

    expect(stampSecondaryEventProfile(stringEvent, 'research').payload).toBe('text')
    expect(stampSecondaryEventProfile(arrayEvent, 'research').payload).toEqual(['text'])
  })
})

describe('getGatewayForProfile', () => {
  it('returns the primary gateway only for the owning profile', () => {
    const researchGateway = gateway('research')

    setPrimaryGateway(researchGateway, 'research')

    expect(getGatewayForProfile('research')).toBe(researchGateway)
    expect(getGatewayForProfile('default')).toBeNull()
  })
})
