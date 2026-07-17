'use client'

import { type ToolCallMessagePartProps, useAuiState } from '@assistant-ui/react'
import { useStore } from '@nanostores/react'
import {
  type ComponentProps,
  type FormEvent,
  type KeyboardEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState
} from 'react'

import { useSessionView } from '@/app/chat/session-view'
import { ToolFallback } from '@/components/assistant-ui/tool/fallback'
import { Button } from '@/components/ui/button'
import { Kbd } from '@/components/ui/kbd'
import { Textarea } from '@/components/ui/textarea'
import { useI18n } from '@/i18n'
import { triggerHaptic } from '@/lib/haptics'
import { Check, CircleLetterA, Loader2, MessageQuestion } from '@/lib/icons'
import { cn } from '@/lib/utils'
import { clearClarifyRequest, sessionClarifyRequest } from '@/store/clarify'
import { $gateway } from '@/store/gateway'
import { notify, notifyError } from '@/store/notifications'
import { $activeSessionId } from '@/store/session'

import { selectMessageRunning } from './tool/fallback-model'
import { parseMaybeObject } from './tool/fallback-model/format'

interface ClarifyArgs {
  allowOther?: boolean
  choices?: string[] | null
  maxSelections?: number | null
  minSelections?: number | null
  multiSelect?: boolean
  question?: string
}

interface ClarifyResult {
  question?: string
  answer?: string
  error?: string
}

const CLARIFY_RESPOND_TIMEOUT_MS = 120_000
const MAX_RESPONSE_CLAIMS = 512

// A pending clarify request can briefly have more than one mounted tool row
// (session tiles, transcript reconciliation, or React remounts). Claim it at
// module scope before crossing the gateway so those rows cannot answer the same
// request concurrently. Accepted/stale claims stay claimed; retryable transport
// failures release their claim. The bounded set prevents unbounded renderer
// lifetime growth while keeping all recent transcript rows protected.
const claimedClarifyResponses = new Set<string>()

function clarifyResponseKey(requestId: string, sessionId: string | null): string {
  return `${sessionId ?? ''}\u0000${requestId}`
}

function claimClarifyResponse(key: string): boolean {
  if (claimedClarifyResponses.has(key)) {
    return false
  }

  claimedClarifyResponses.add(key)

  if (claimedClarifyResponses.size > MAX_RESPONSE_CLAIMS) {
    const oldest = claimedClarifyResponses.values().next().value

    if (oldest !== undefined) {
      claimedClarifyResponses.delete(oldest)
    }
  }

  return true
}

function releaseClarifyResponse(key: string): void {
  claimedClarifyResponses.delete(key)
}

function readNumber(row: Record<string, unknown>, camel: string, snake: string): number | null | undefined {
  const value = row[camel] !== undefined ? row[camel] : row[snake]

  return typeof value === 'number' ? value : value === null ? null : undefined
}

function readBoolean(row: Record<string, unknown>, camel: string, snake: string): boolean | undefined {
  const value = row[camel] !== undefined ? row[camel] : row[snake]

  return typeof value === 'boolean' ? value : undefined
}

function stringField(row: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const key of keys) {
    const value = row[key]

    if (typeof value === 'string') {
      return value
    }
  }
}

function isClarifyRespondTimeoutError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error)

  return /request timed out:\s*clarify\.respond/i.test(message)
}

function isClarifyNoPendingRequestError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error)

  return /no pending answer request/i.test(message)
}

function readClarifyArgs(args: unknown): ClarifyArgs {
  const row = parseMaybeObject(args)

  const choices = Array.isArray(row.choices)
    ? row.choices.filter((choice): choice is string => typeof choice === 'string')
    : null

  return {
    allowOther: readBoolean(row, 'allowOther', 'allow_other'),
    choices: choices && choices.length > 0 ? choices : null,
    maxSelections: readNumber(row, 'maxSelections', 'max_selections'),
    minSelections: readNumber(row, 'minSelections', 'min_selections'),
    multiSelect: readBoolean(row, 'multiSelect', 'multi_select'),
    question: stringField(row, 'question')
  }
}

/** Parse clarify tool JSON (`question` + `user_response`). */
export function readClarifyResult(result: unknown): ClarifyResult {
  const row = parseMaybeObject(result)

  if (Object.keys(row).length === 0) {
    return typeof result === 'string' && result.trim() ? { answer: result.trim() } : {}
  }

  return {
    question: stringField(row, 'question'),
    answer: stringField(row, 'user_response', 'answer'),
    error: stringField(row, 'error')
  }
}

const letterFor = (index: number): string => String.fromCharCode(65 + index)

const OPTION_ROW_CLASS =
  'flex w-full items-start gap-2 rounded-[0.25rem] px-1.5 py-1 text-left disabled:cursor-not-allowed disabled:opacity-50'

// field-sizing on top of Textarea's shared chrome; kill min-h-16 for one-liners.
const CLARIFY_TEXTAREA_CLASS = 'field-sizing-content max-h-40 min-h-0 resize-none'

const CLARIFY_SHELL_CLASS =
  'my-1.5 rounded-md border border-primary/20 bg-(--ui-chat-surface-background) text-[length:var(--conversation-text-font-size)] text-(--ui-text-primary)'

const CLARIFY_ICON_CLASS = 'mt-px size-4 shrink-0 text-(--ui-text-tertiary)'

function ClarifyShell({ children, className, ...props }: ComponentProps<'div'>) {
  return (
    <div className={cn(CLARIFY_SHELL_CLASS, className)} data-slot="clarify-inline" {...props}>
      {children}
    </div>
  )
}

function ClarifyLine({
  children,
  className,
  icon: Icon,
  ...props
}: ComponentProps<'div'> & { icon: typeof MessageQuestion }) {
  return (
    <div className={cn('flex items-start gap-2', className)} {...props}>
      <div className="min-w-0 flex-1">{children}</div>
      <Icon aria-hidden className={CLARIFY_ICON_CLASS} />
    </div>
  )
}

function KeyBadge({ char, preview, selected }: { char: string; preview?: boolean; selected: boolean }) {
  return (
    <Kbd
      className={cn(
        'mt-px',
        selected && 'border-primary bg-primary text-white shadow-none',
        !selected && preview && 'border-primary text-primary shadow-none'
      )}
      size="sm"
    >
      {char}
    </Kbd>
  )
}

function SelectToggle({ selected }: { selected: boolean }) {
  return (
    <span
      aria-hidden
      className={cn(
        'mt-px grid size-4 shrink-0 place-items-center rounded-full border transition-colors',
        selected ? 'border-primary bg-primary text-white' : 'border-(--ui-stroke-secondary) text-transparent'
      )}
    >
      {selected && <Check className="size-3" />}
    </span>
  )
}

function toggleChoice(choices: string[], choice: string, maxSelections: number | null): string[] {
  if (choices.includes(choice)) {
    return choices.filter(item => item !== choice)
  }

  if (maxSelections !== null && choices.length >= maxSelections) {
    return choices
  }

  return [...choices, choice]
}

export const ClarifyTool = (props: ToolCallMessagePartProps) => {
  // Answered → settled Q&A (ToolFallback collapsed the answer away).
  if (props.result !== undefined) {
    return <ClarifyToolSettled {...props} />
  }

  return <ClarifyToolLive {...props} />
}

function ClarifyToolLive(props: ToolCallMessagePartProps) {
  const messageRunning = useAuiState(selectMessageRunning)

  // Stopped mid-prompt with no result — don't leave a dead interactive panel.
  if (!messageRunning) {
    return <ToolFallback {...props} />
  }

  return <ClarifyToolPending {...props} />
}

function ClarifyToolSettled({ args, result }: ToolCallMessagePartProps) {
  const { t } = useI18n()
  const copy = t.assistant.clarify
  const fromArgs = useMemo(() => readClarifyArgs(args), [args])
  const fromResult = useMemo(() => readClarifyResult(result), [result])

  const question = fromResult.question || fromArgs.question || ''
  const answer = fromResult.answer
  const error = fromResult.error
  const skipped = !error && answer !== undefined && !answer.trim()
  const answerText = error || (skipped ? copy.skipped : (answer ?? '').trim())

  return (
    <ClarifyShell className="grid gap-1.5 px-2.5 py-2" data-clarify-settled="">
      {question ? (
        <ClarifyLine icon={MessageQuestion}>
          <span className="whitespace-pre-wrap font-medium leading-(--conversation-line-height)">{question}</span>
        </ClarifyLine>
      ) : null}
      {answerText ? (
        <ClarifyLine icon={CircleLetterA}>
          <p
            className={cn(
              'whitespace-pre-wrap leading-(--conversation-line-height)',
              error ? 'text-destructive' : 'text-(--ui-text-secondary)',
              skipped && 'italic text-(--ui-text-tertiary)'
            )}
            data-clarify-answer=""
          >
            {answerText}
          </p>
        </ClarifyLine>
      ) : null}
    </ClarifyShell>
  )
}

function ClarifyToolPending({ args }: ToolCallMessagePartProps) {
  const { t } = useI18n()
  const copy = t.assistant.clarify
  // The tool row is in whichever session's transcript rendered it — read THAT
  // session's clarify (primary or tile), not the globally-active one.
  const sessionId = useStore(useSessionView().$runtimeId)
  const activeSessionId = useStore($activeSessionId)
  const $request = useMemo(() => sessionClarifyRequest(sessionId), [sessionId])
  const request = useStore($request)
  const gateway = useStore($gateway)
  const fromArgs = useMemo(() => readClarifyArgs(args), [args])

  const matchingRequest = useMemo(() => {
    if (!request) {
      return null
    }

    if (fromArgs.question && request.question && fromArgs.question !== request.question) {
      return null
    }

    return request
  }, [fromArgs.question, request])

  const question = fromArgs.question || matchingRequest?.question || ''

  const choices = useMemo(
    () => fromArgs.choices ?? matchingRequest?.choices ?? [],
    [fromArgs.choices, matchingRequest?.choices]
  )

  const hasChoices = choices.length > 0
  const multiSelect = fromArgs.multiSelect ?? matchingRequest?.multiSelect ?? false
  const allowOther = fromArgs.allowOther ?? matchingRequest?.allowOther ?? true
  const minSelections = Math.max(0, Math.trunc(fromArgs.minSelections ?? matchingRequest?.minSelections ?? 0))
  const configuredMaxSelections = fromArgs.maxSelections ?? matchingRequest?.maxSelections ?? null
  const maxSelections = configuredMaxSelections === null ? null : Math.max(0, Math.trunc(configuredMaxSelections))

  const [draft, setDraft] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [selectedChoice, setSelectedChoice] = useState<string | null>(null)
  const [selectedChoices, setSelectedChoices] = useState<string[]>([])
  const [otherFocused, setOtherFocused] = useState(false)
  const [expired, setExpired] = useState(false)
  const submittingRef = useRef(false)
  const textareaRef = useRef<HTMLTextAreaElement | null>(null)

  // Race: tool.start fires a tick before clarify.request, so request_id
  // arrives slightly after the tool block mounts. Hold the whole panel on a
  // spinner until the gateway request is wired — showing disabled choices or
  // a "loading question" stub is worse than a brief wait.
  const ready = Boolean(matchingRequest?.requestId)
  const loading = !ready && !submitting

  const respond = useCallback(
    async (answer: string) => {
      if (!ready || !matchingRequest) {
        notifyError(new Error(copy.notReady), copy.sendFailed)

        return
      }

      if (!gateway) {
        notifyError(new Error(copy.gatewayDisconnected), copy.sendFailed)

        return
      }

      const responseKey = clarifyResponseKey(matchingRequest.requestId, matchingRequest.sessionId)

      if (submittingRef.current || !claimClarifyResponse(responseKey)) {
        return
      }

      submittingRef.current = true
      setSubmitting(true)

      try {
        await gateway.request<{ ok?: boolean }>(
          'clarify.respond',
          {
            request_id: matchingRequest.requestId,
            answer
          },
          CLARIFY_RESPOND_TIMEOUT_MS
        )
        triggerHaptic('submit')
        clearClarifyRequest(matchingRequest.requestId, matchingRequest.sessionId)
        // tool.complete lands next → ClarifyToolSettled.
      } catch (error) {
        if (isClarifyRespondTimeoutError(error)) {
          notify({
            kind: 'warning',
            title: copy.responsePendingTitle,
            message: copy.responsePendingMessage,
            detail: error instanceof Error ? error.message : String(error),
            durationMs: 12_000
          })
          releaseClarifyResponse(responseKey)
          submittingRef.current = false
          setSubmitting(false)
        } else if (isClarifyNoPendingRequestError(error)) {
          clearClarifyRequest(matchingRequest.requestId, matchingRequest.sessionId)
          setExpired(true)
          notify({
            kind: 'warning',
            title: copy.responseExpiredTitle,
            message: copy.responseExpiredMessage,
            detail: error instanceof Error ? error.message : String(error),
            durationMs: 12_000
          })
        } else {
          notifyError(error, copy.sendFailed)
          releaseClarifyResponse(responseKey)
          submittingRef.current = false
          setSubmitting(false)
        }
      }
    },
    [
      copy.gatewayDisconnected,
      copy.notReady,
      copy.responseExpiredMessage,
      copy.responseExpiredTitle,
      copy.responsePendingMessage,
      copy.responsePendingTitle,
      copy.sendFailed,
      gateway,
      matchingRequest,
      ready
    ]
  )

  const trimmedDraft = draft.trim()
  const selectedSummary = selectedChoices.join(', ')
  const customSummary = trimmedDraft ? `${copy.other}: ${trimmedDraft}` : ''
  const selectionSummary = selectedSummary || selectedChoice || customSummary
  const selectedChoiceCount = selectedChoices.length
  const canSubmitSelected = multiSelect && selectedChoiceCount > 0 && selectedChoiceCount >= minSelections
  const canSkip = minSelections <= 0 && (multiSelect || !hasChoices || allowOther)
  const selectionLimitReached = maxSelections !== null && selectedChoiceCount >= maxSelections

  const selectChoice = useCallback(
    (choice: string) => {
      setDraft('')
      setSelectedChoices([])
      setSelectedChoice(choice)
      void respond(choice)
    },
    [respond]
  )

  const toggleMultiChoice = useCallback(
    (choice: string) => {
      if (!multiSelect) {
        return
      }

      setDraft('')
      setSelectedChoice(null)
      setSelectedChoices(current => toggleChoice(current, choice, maxSelections))
    },
    [maxSelections, multiSelect]
  )

  const submitSelected = useCallback(() => {
    if (canSubmitSelected) {
      void respond(selectedChoices.join(', '))
    }
  }, [canSubmitSelected, respond, selectedChoices])

  const submitDraft = useCallback(() => {
    if (trimmedDraft) {
      void respond(trimmedDraft)
    }
  }, [respond, trimmedDraft])

  const handleTextareaKey = useCallback(
    (event: KeyboardEvent<HTMLTextAreaElement>) => {
      if (event.nativeEvent.isComposing) {
        return
      }

      if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault()
        submitDraft()
      }
    },
    [submitDraft]
  )

  const handleSubmitDraft = useCallback(
    (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault()
      submitDraft()
    },
    [submitDraft]
  )

  // Letter shortcuts are owned by the active session only. Multiple session
  // tiles may have live clarify panels at once; without this gate the same key
  // would answer whichever effect happened to mount first (or more than one).
  useEffect(() => {
    if (!ready || !hasChoices || submitting || expired || sessionId !== activeSessionId) {
      return
    }

    const handleShortcut = (event: globalThis.KeyboardEvent) => {
      if (event.metaKey || event.ctrlKey || event.altKey || event.defaultPrevented) {
        return
      }

      const active = document.activeElement as HTMLElement | null
      const tag = active?.tagName.toLowerCase()

      if (tag === 'input' || tag === 'textarea' || active?.isContentEditable) {
        return
      }

      if (event.key.length === 1) {
        const index = event.key.toUpperCase().charCodeAt(0) - 65

        if (index >= 0 && index < choices.length) {
          event.preventDefault()

          if (multiSelect) {
            toggleMultiChoice(choices[index])
          } else {
            selectChoice(choices[index])
          }

          return
        }

        if (allowOther && index === choices.length) {
          event.preventDefault()
          textareaRef.current?.focus()
        }

        return
      }

      if (event.key === 'Enter' && multiSelect && canSubmitSelected && !trimmedDraft) {
        event.preventDefault()
        submitSelected()
      }
    }

    window.addEventListener('keydown', handleShortcut)

    return () => window.removeEventListener('keydown', handleShortcut)
  }, [
    activeSessionId,
    allowOther,
    canSubmitSelected,
    choices,
    expired,
    hasChoices,
    multiSelect,
    ready,
    selectChoice,
    sessionId,
    submitSelected,
    submitting,
    toggleMultiChoice,
    trimmedDraft
  ])

  if (expired) {
    return (
      <ClarifyShell className="grid gap-1 px-2.5 py-2" role="status">
        <div className="font-medium text-(--ui-text-primary)">{copy.responseExpiredTitle}</div>
        <div className="text-xs text-(--ui-text-secondary)">{copy.responseExpiredMessage}</div>
      </ClarifyShell>
    )
  }

  if (loading) {
    return (
      <ClarifyShell
        aria-label={copy.loadingQuestion}
        className="grid min-h-12 place-items-center px-2.5 py-3"
        role="status"
      >
        <Loader2 aria-hidden className="size-4 animate-spin text-(--ui-text-tertiary)" />
      </ClarifyShell>
    )
  }

  const onDraftChange = (value: string) => {
    setDraft(value)

    // Typing is its own answer — drop any picked/staged choice so the inputs
    // can't both look selected.
    if (value.trim()) {
      setSelectedChoice(null)
      setSelectedChoices([])
    }
  }

  return (
    <ClarifyShell className="grid gap-2 px-2.5 py-2">
      <div className="flex items-start gap-2">
        <span className="flex-1 whitespace-pre-wrap font-medium leading-(--conversation-line-height)">{question}</span>
        <MessageQuestion aria-hidden className="mt-px size-4 shrink-0 text-(--ui-text-tertiary)" />
      </div>

      {selectionSummary && (
        <div className="rounded-[0.25rem] border border-primary/20 bg-primary/5 px-2 py-1 text-xs" role="status">
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium text-primary">{copy.selected}</span>
            {selectedChoiceCount > 0 && (
              <span className="text-(--ui-text-tertiary)">{copy.selectedCount(selectedChoiceCount)}</span>
            )}
          </div>
          <div className="mt-0.5 wrap-anywhere text-(--ui-text-secondary)">{selectionSummary}</div>
        </div>
      )}

      {hasChoices && multiSelect && (
        <div
          className="rounded-[0.25rem] bg-(--chrome-action-hover) px-2 py-1 text-xs text-(--ui-text-secondary)"
          role="note"
        >
          {copy.multiSelectHint}
        </div>
      )}

      {hasChoices && (
        <div className="grid gap-px" role="group">
          {choices.map((choice, index) => {
            const staged = selectedChoices.includes(choice)

            if (multiSelect) {
              return (
                <button
                  aria-label={`Toggle ${choice} for multi-select`}
                  aria-pressed={staged}
                  className={cn(
                    OPTION_ROW_CLASS,
                    'text-(--ui-text-secondary) hover:bg-(--chrome-action-hover) hover:text-(--ui-text-primary)',
                    staged && 'text-(--ui-text-primary)'
                  )}
                  data-choice
                  disabled={submitting || (selectionLimitReached && !staged)}
                  key={`${index}-${choice}`}
                  onClick={() => toggleMultiChoice(choice)}
                  type="button"
                >
                  <KeyBadge char={letterFor(index)} selected={staged} />
                  <span className="flex-1 wrap-anywhere">{choice}</span>
                  <SelectToggle selected={staged} />
                </button>
              )
            }

            return (
              <button
                aria-label={choice}
                className={cn(
                  OPTION_ROW_CLASS,
                  'text-(--ui-text-secondary) hover:bg-(--chrome-action-hover) hover:text-(--ui-text-primary)',
                  selectedChoice === choice && 'text-(--ui-text-primary)'
                )}
                data-choice
                disabled={submitting}
                key={`${index}-${choice}`}
                onClick={() => selectChoice(choice)}
                type="button"
              >
                <KeyBadge char={letterFor(index)} selected={selectedChoice === choice} />
                <span className="flex-1 wrap-anywhere">{choice}</span>
              </button>
            )
          })}
          {allowOther && (
            <label className={cn(OPTION_ROW_CLASS, 'items-center focus-within:bg-(--chrome-action-hover)')}>
              <KeyBadge char={letterFor(choices.length)} preview={otherFocused} selected={Boolean(trimmedDraft)} />
              <Textarea
                aria-label={copy.other}
                className={CLARIFY_TEXTAREA_CLASS}
                disabled={submitting}
                onBlur={() => setOtherFocused(false)}
                onChange={event => onDraftChange(event.target.value)}
                onFocus={() => {
                  setSelectedChoice(null)
                  setSelectedChoices([])
                  setOtherFocused(true)
                }}
                onKeyDown={handleTextareaKey}
                placeholder={copy.other}
                ref={textareaRef}
                rows={1}
                size="sm"
                value={draft}
              />
            </label>
          )}
        </div>
      )}

      {!hasChoices && (
        <form className="grid gap-2" onSubmit={handleSubmitDraft}>
          <Textarea
            className={CLARIFY_TEXTAREA_CLASS}
            disabled={submitting}
            onChange={event => onDraftChange(event.target.value)}
            onKeyDown={handleTextareaKey}
            placeholder={copy.placeholder}
            ref={textareaRef}
            rows={1}
            size="sm"
            value={draft}
          />
        </form>
      )}

      <div className="flex items-center justify-end gap-1">
        {canSkip && (
          <Button disabled={submitting} onClick={() => void respond('')} size="xs" type="button" variant="text">
            {copy.skip}
          </Button>
        )}
        {multiSelect && !trimmedDraft ? (
          <Button disabled={submitting || !canSubmitSelected} onClick={submitSelected} size="xs" type="button">
            {submitting ? <Loader2 className="size-3 animate-spin" /> : copy.selectSelected}
          </Button>
        ) : (
          <Button disabled={submitting || !trimmedDraft} onClick={submitDraft} size="xs" type="button">
            {submitting ? (
              <Loader2 className="size-3 animate-spin" />
            ) : (
              <>
                {copy.continueLabel}
                <span aria-hidden className="ml-0.5 text-[0.625rem] opacity-70">
                  ⏎
                </span>
              </>
            )}
          </Button>
        )}
      </div>
    </ClarifyShell>
  )
}
