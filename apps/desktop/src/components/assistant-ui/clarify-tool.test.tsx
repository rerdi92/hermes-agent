import type { ToolCallMessagePartProps } from '@assistant-ui/react'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { atom } from 'nanostores'
import type { ReactNode } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { PRIMARY_SESSION_VIEW, SessionViewProvider } from '@/app/chat/session-view'
import { I18nProvider } from '@/i18n'
import { $clarifyRequests, clearClarifyRequest, setClarifyRequest } from '@/store/clarify'
import { $gateway } from '@/store/gateway'
import { $notifications, clearNotifications } from '@/store/notifications'
import { $activeSessionId } from '@/store/session'

import { ClarifyTool, readClarifyResult } from './clarify-tool'

vi.mock('@assistant-ui/react', () => ({
  useAuiState: () => true
}))

vi.mock('@/lib/haptics', () => ({
  triggerHaptic: vi.fn()
}))

type TestClarifyArgs = {
  allowOther?: boolean
  choices: string[]
  maxSelections?: number | null
  minSelections?: number | null
  multiSelect?: boolean
  question: string
}

type RequestMock = ReturnType<typeof vi.fn>

let requestSequence = 0

function resetClarifyTestState() {
  clearNotifications()
  clearClarifyRequest()
  $clarifyRequests.set({})
  $activeSessionId.set(null)
  $gateway.set(null)
}

function sessionView(sessionId: string | null) {
  return {
    ...PRIMARY_SESSION_VIEW,
    kind: 'tile' as const,
    $runtimeId: atom(sessionId)
  }
}

function renderClarify(ui: ReactNode) {
  return render(
    <I18nProvider configClient={null} initialLocale="en">
      {ui}
    </I18nProvider>
  )
}

function liveClarifyProps(args: ToolCallMessagePartProps['args'], toolCallId = 'tool-1'): ToolCallMessagePartProps {
  return {
    addResult: vi.fn(),
    args,
    argsText: JSON.stringify(args),
    isError: false,
    respondToApproval: vi.fn(),
    result: undefined,
    resume: vi.fn(),
    status: { type: 'running' },
    toolCallId,
    toolName: 'clarify',
    type: 'tool-call'
  }
}

function settledClarifyProps(
  args: ToolCallMessagePartProps['args'],
  result: ToolCallMessagePartProps['result'],
  toolCallId: string
): ToolCallMessagePartProps {
  return {
    ...liveClarifyProps(args, toolCallId),
    result,
    status: { type: 'complete' }
  }
}

function renderClarifyTool(
  requestMock: RequestMock = vi.fn().mockResolvedValue({ ok: true }),
  argsOverride: Partial<TestClarifyArgs> & Record<string, unknown> = {},
  options: { activeSessionId?: string | null; requestId?: string; sessionId?: string | null } = {}
) {
  const args = {
    question: 'Which context should Hermes use?',
    choices: ['Past sessions', 'Local reports', 'Web sources'],
    ...argsOverride
  } as TestClarifyArgs & Record<string, unknown>

  const sessionId = options.sessionId ?? null
  const activeSessionId = options.activeSessionId === undefined ? sessionId : options.activeSessionId
  const requestId = options.requestId ?? `req-${++requestSequence}`

  $gateway.set({ request: requestMock } as never)
  $activeSessionId.set(activeSessionId)
  setClarifyRequest({
    allowOther: args.allowOther,
    choices: args.choices,
    maxSelections: args.maxSelections,
    minSelections: args.minSelections,
    multiSelect: args.multiSelect,
    question: args.question,
    requestId,
    sessionId
  })

  renderClarify(
    <SessionViewProvider value={sessionView(sessionId)}>
      <ClarifyTool {...liveClarifyProps(args)} />
    </SessionViewProvider>
  )

  return { requestId, requestMock }
}

beforeEach(() => {
  resetClarifyTestState()
})

afterEach(() => {
  cleanup()
  resetClarifyTestState()
  vi.restoreAllMocks()
})

describe('readClarifyResult', () => {
  it('reads question + user_response from the tool JSON payload', () => {
    expect(
      readClarifyResult({
        question: 'Which target?',
        choices_offered: ['staging', 'prod'],
        user_response: 'staging'
      })
    ).toEqual({
      question: 'Which target?',
      answer: 'staging',
      error: undefined
    })
  })

  it('parses a JSON string result the same way as an object', () => {
    expect(
      readClarifyResult(
        JSON.stringify({
          question: 'Ship it?',
          user_response: 'yes'
        })
      )
    ).toEqual({
      question: 'Ship it?',
      answer: 'yes',
      error: undefined
    })
  })

  it('keeps an empty user_response so Skip can render as skipped', () => {
    expect(readClarifyResult({ question: 'Ok?', user_response: '' })).toEqual({
      question: 'Ok?',
      answer: '',
      error: undefined
    })
  })
})

describe('ClarifyTool settled view', () => {
  it('keeps the question and answer visible after the tool completes', () => {
    renderClarify(
      <ClarifyTool
        {...settledClarifyProps(
          { question: 'Which deployment target?', choices: ['staging', 'prod'] },
          {
            question: 'Which deployment target?',
            choices_offered: ['staging', 'prod'],
            user_response: 'staging'
          },
          'clarify-1'
        )}
      />
    )

    expect(screen.getByText('Which deployment target?')).toBeTruthy()
    expect(screen.getByText('staging')).toBeTruthy()
    expect(globalThis.document.querySelector('[data-clarify-settled]')).toBeTruthy()
    expect(globalThis.document.querySelector('[data-clarify-answer]')?.textContent).toBe('staging')
  })

  it('labels an empty response as Skipped', () => {
    renderClarify(
      <ClarifyTool
        {...settledClarifyProps(
          { question: 'Anything else?' },
          { question: 'Anything else?', user_response: '' },
          'clarify-2'
        )}
      />
    )

    expect(screen.getByText('Anything else?')).toBeTruthy()
    expect(screen.getByText('Skipped')).toBeTruthy()
  })
})

describe('ClarifyTool selection status UX', () => {
  it('submits regular single choices immediately', async () => {
    const { requestId, requestMock } = renderClarifyTool()

    fireEvent.click(screen.getByRole('button', { name: 'Past sessions' }))

    await waitFor(() =>
      expect(requestMock).toHaveBeenCalledWith(
        'clarify.respond',
        {
          request_id: requestId,
          answer: 'Past sessions'
        },
        120_000
      )
    )
  })

  it('shows the selected single choice while the response is pending', () => {
    const { requestMock } = renderClarifyTool(vi.fn(() => new Promise(() => {})))

    fireEvent.click(screen.getByRole('button', { name: 'Past sessions' }))

    expect(requestMock).toHaveBeenCalledTimes(1)
    const status = screen.getByRole('status')
    expect(status.textContent).toContain('Selected')
    expect(status.textContent).toContain('Past sessions')
  })

  it('submits a single-choice answer from its letter shortcut', async () => {
    const { requestId, requestMock } = renderClarifyTool()

    fireEvent.keyDown(window, { key: 'b' })

    await waitFor(() =>
      expect(requestMock).toHaveBeenCalledWith(
        'clarify.respond',
        {
          request_id: requestId,
          answer: 'Local reports'
        },
        120_000
      )
    )
  })

  it('focuses Other from the trailing letter shortcut and submits it with Enter', async () => {
    const { requestId, requestMock } = renderClarifyTool()

    fireEvent.keyDown(window, { key: 'd' })

    const other = screen.getByRole('textbox', { name: 'Other (type your answer)' })
    expect(globalThis.document.activeElement).toBe(other)

    fireEvent.change(other, { target: { value: 'Use a custom path' } })
    fireEvent.keyDown(other, { key: 'Enter' })

    await waitFor(() =>
      expect(requestMock).toHaveBeenCalledWith(
        'clarify.respond',
        {
          request_id: requestId,
          answer: 'Use a custom path'
        },
        120_000
      )
    )
  })

  it('does not expose staged multi-select controls for single-choice prompts', () => {
    renderClarifyTool()

    expect(screen.queryByRole('button', { name: 'Toggle Past sessions for multi-select' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Select selected' })).toBeNull()
  })

  it('keeps the multi-select submit button visible before any choice is staged', () => {
    renderClarifyTool(vi.fn().mockResolvedValue({ ok: true }), { multiSelect: true })

    expect(screen.getByRole('note').textContent).toContain('Multi-select')
    expect(screen.getByRole('button', { name: 'Select selected' }).hasAttribute('disabled')).toBe(true)
  })

  it('stages multiple choices and summarizes them before submitting', async () => {
    const { requestId, requestMock } = renderClarifyTool(vi.fn().mockResolvedValue({ ok: true }), {
      multiSelect: true
    })

    fireEvent.click(screen.getByRole('button', { name: 'Toggle Past sessions for multi-select' }))
    fireEvent.click(screen.getByRole('button', { name: 'Toggle Web sources for multi-select' }))

    expect(requestMock).not.toHaveBeenCalled()
    expect(screen.getByText('2 selected')).toBeTruthy()
    expect(screen.getByText('Selected')).toBeTruthy()
    expect(screen.getByText('Past sessions, Web sources')).toBeTruthy()

    fireEvent.click(screen.getByRole('button', { name: 'Select selected' }))

    await waitFor(() =>
      expect(requestMock).toHaveBeenCalledWith(
        'clarify.respond',
        {
          request_id: requestId,
          answer: 'Past sessions, Web sources'
        },
        120_000
      )
    )
  })

  it('enforces min/max selections and accepts snake_case constraints', () => {
    renderClarifyTool(vi.fn().mockResolvedValue({ ok: true }), {
      allow_other: false,
      max_selections: 2,
      min_selections: 2,
      multi_select: true
    })

    const first = screen.getByRole('button', { name: 'Toggle Past sessions for multi-select' })
    const second = screen.getByRole('button', { name: 'Toggle Local reports for multi-select' })
    const third = screen.getByRole('button', { name: 'Toggle Web sources for multi-select' })
    const submit = screen.getByRole('button', { name: 'Select selected' })

    expect(screen.queryByRole('textbox', { name: 'Other (type your answer)' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Skip' })).toBeNull()
    expect(submit.hasAttribute('disabled')).toBe(true)

    fireEvent.click(first)
    expect(submit.hasAttribute('disabled')).toBe(true)

    fireEvent.click(second)
    expect(submit.hasAttribute('disabled')).toBe(false)
    expect(third.hasAttribute('disabled')).toBe(true)
    fireEvent.click(third)

    expect(screen.getByText('Past sessions, Local reports')).toBeTruthy()
    expect(screen.queryByText('Past sessions, Local reports, Web sources')).toBeNull()
  })

  it('toggles multi-select choices by letter shortcut and submits them with Enter', async () => {
    const { requestId, requestMock } = renderClarifyTool(vi.fn().mockResolvedValue({ ok: true }), {
      multiSelect: true
    })

    fireEvent.keyDown(window, { key: 'a' })
    fireEvent.keyDown(window, { key: 'c' })

    expect(requestMock).not.toHaveBeenCalled()
    expect(screen.getByText('2 selected')).toBeTruthy()
    expect(screen.getByText('Past sessions, Web sources')).toBeTruthy()

    fireEvent.keyDown(window, { key: 'Enter' })

    await waitFor(() =>
      expect(requestMock).toHaveBeenCalledWith(
        'clarify.respond',
        {
          request_id: requestId,
          answer: 'Past sessions, Web sources'
        },
        120_000
      )
    )
  })

  it('keeps free-form Other submission available for multi-select prompts', async () => {
    const { requestId, requestMock } = renderClarifyTool(vi.fn().mockResolvedValue({ ok: true }), {
      multiSelect: true
    })

    const other = screen.getByRole('textbox', { name: 'Other (type your answer)' })
    fireEvent.focus(other)
    fireEvent.change(other, { target: { value: 'Use a custom path' } })

    expect(screen.getByText('Other (type your answer): Use a custom path')).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }))

    await waitFor(() =>
      expect(requestMock).toHaveBeenCalledWith(
        'clarify.respond',
        {
          request_id: requestId,
          answer: 'Use a custom path'
        },
        120_000
      )
    )
  })
})

describe('ClarifyTool request identity and acknowledgement handling', () => {
  it('claims a pending request exactly once across duplicate live tool rows', () => {
    const requestMock = vi.fn(() => new Promise(() => {}))
    const requestId = `req-${++requestSequence}`
    const sessionId = 'duplicate-session'

    const args = {
      question: 'Which context should Hermes use?',
      choices: ['Past sessions', 'Local reports', 'Web sources']
    }

    $activeSessionId.set(sessionId)
    $gateway.set({ request: requestMock } as never)
    setClarifyRequest({ ...args, requestId, sessionId })

    renderClarify(
      <>
        <SessionViewProvider value={sessionView(sessionId)}>
          <ClarifyTool {...liveClarifyProps(args, 'duplicate-tool-1')} />
        </SessionViewProvider>
        <SessionViewProvider value={sessionView(sessionId)}>
          <ClarifyTool {...liveClarifyProps(args, 'duplicate-tool-2')} />
        </SessionViewProvider>
      </>
    )

    const buttons = screen.getAllByRole('button', { name: 'Past sessions' })
    fireEvent.click(buttons[0])
    fireEvent.click(buttons[1])

    expect(requestMock).toHaveBeenCalledTimes(1)
    expect(requestMock).toHaveBeenCalledWith(
      'clarify.respond',
      { request_id: requestId, answer: 'Past sessions' },
      120_000
    )
  })

  it('routes each panel to its own session request and shortcuts only the active session', async () => {
    const requestMock = vi.fn().mockResolvedValue({ ok: true })
    const firstSession = 'session-one'
    const secondSession = 'session-two'
    const firstRequest = `req-${++requestSequence}`
    const secondRequest = `req-${++requestSequence}`
    const firstArgs = { question: 'First session?', choices: ['Use first session'] }
    const secondArgs = { question: 'Second session?', choices: ['Use second session'] }

    $activeSessionId.set(firstSession)
    $gateway.set({ request: requestMock } as never)
    setClarifyRequest({ ...firstArgs, requestId: firstRequest, sessionId: firstSession })
    setClarifyRequest({ ...secondArgs, requestId: secondRequest, sessionId: secondSession })

    renderClarify(
      <>
        <SessionViewProvider value={sessionView(firstSession)}>
          <ClarifyTool {...liveClarifyProps(firstArgs, 'first-tool')} />
        </SessionViewProvider>
        <SessionViewProvider value={sessionView(secondSession)}>
          <ClarifyTool {...liveClarifyProps(secondArgs, 'second-tool')} />
        </SessionViewProvider>
      </>
    )

    fireEvent.keyDown(window, { key: 'a' })

    await waitFor(() => expect(requestMock).toHaveBeenCalledTimes(1))
    expect(requestMock).toHaveBeenLastCalledWith(
      'clarify.respond',
      { request_id: firstRequest, answer: 'Use first session' },
      120_000
    )

    const secondShell = screen.getByText('Second session?').closest('[data-slot="clarify-inline"]')
    expect(secondShell).toBeTruthy()
    fireEvent.click(within(secondShell as HTMLElement).getByRole('button', { name: 'Use second session' }))

    await waitFor(() => expect(requestMock).toHaveBeenCalledTimes(2))
    expect(requestMock).toHaveBeenLastCalledWith(
      'clarify.respond',
      { request_id: secondRequest, answer: 'Use second session' },
      120_000
    )
  })

  it('uses a 120 second timeout and warns when acknowledgement times out', async () => {
    const { requestId, requestMock } = renderClarifyTool(
      vi.fn(async () => {
        throw new Error('request timed out: clarify.respond')
      })
    )

    fireEvent.click(screen.getByRole('button', { name: 'Past sessions' }))

    await waitFor(() =>
      expect(requestMock).toHaveBeenCalledWith(
        'clarify.respond',
        { request_id: requestId, answer: 'Past sessions' },
        120_000
      )
    )
    await waitFor(() => {
      expect($notifications.get()[0]).toMatchObject({
        kind: 'warning',
        title: 'Clarify response may still be processing',
        message: expect.stringContaining('wait a moment before trying again')
      })
    })
  })

  it('clears only the stale session request when the backend has no pending request', async () => {
    const sessionId = 'stale-session'
    const otherSessionId = 'other-session'

    const { requestMock } = renderClarifyTool(
      vi.fn(async () => {
        throw new Error('RPC 4009: no pending answer request')
      }),
      {},
      { sessionId }
    )

    setClarifyRequest({
      choices: ['Keep me'],
      question: 'Other request?',
      requestId: 'other-request',
      sessionId: otherSessionId
    })

    fireEvent.click(screen.getByRole('button', { name: 'Past sessions' }))

    await waitFor(() => expect(requestMock).toHaveBeenCalled())
    await waitFor(() => {
      expect($clarifyRequests.get()[sessionId]).toBeUndefined()
      expect($clarifyRequests.get()[otherSessionId]?.requestId).toBe('other-request')
      expect(screen.getByText('Clarify request expired')).toBeTruthy()
      expect($notifications.get()[0]).toMatchObject({
        kind: 'warning',
        title: 'Clarify request expired',
        message: expect.stringContaining('no longer pending')
      })
    })
  })
})
