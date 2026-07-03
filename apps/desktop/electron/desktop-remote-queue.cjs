'use strict'

const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')

const DESKTOP_REMOTE_EVENT_CHANNEL = 'hermes:desktop-remote-request'

function resolveDesktopRemoteQueuePath(hermesHome) {
  if (!hermesHome || typeof hermesHome !== 'string') {
    throw new Error('hermesHome is required')
  }
  return path.join(hermesHome, 'state', 'desktop-remote-requests.jsonl')
}

function resolveDesktopRemoteAckPath(hermesHome) {
  if (!hermesHome || typeof hermesHome !== 'string') {
    throw new Error('hermesHome is required')
  }
  return path.join(hermesHome, 'state', 'desktop-remote-acks.jsonl')
}

function safeString(value) {
  return typeof value === 'string' ? value.trim() : ''
}

function normalizeDesktopRemoteRequest(value) {
  if (!value || typeof value !== 'object') return null
  const text = safeString(value.text)
  if (!text) return null
  const source = safeString(value.source) || 'discord'
  const messageId = safeString(value.messageId) || safeString(value.message_id)
  const id = safeString(value.id) || `${source}-${messageId || Date.now()}-${crypto.randomBytes(4).toString('hex')}`
  return {
    autoSubmit: value.autoSubmit === true || value.auto_submit === true,
    channelId: safeString(value.channelId) || safeString(value.channel_id),
    createdAt: Number.isFinite(Number(value.createdAt ?? value.created_at)) ? Number(value.createdAt ?? value.created_at) : Date.now(),
    id,
    messageId,
    source,
    text,
    userId: safeString(value.userId) || safeString(value.user_id),
    userName: safeString(value.userName) || safeString(value.user_name) || safeString(value.userId) || 'Discord user'
  }
}

function appendDesktopRemoteRequest(queuePath, request, options = {}) {
  const fsImpl = options.fs || fs
  const normalized = normalizeDesktopRemoteRequest(request)
  if (!normalized) {
    throw new Error('desktop remote request requires non-empty text')
  }
  fsImpl.mkdirSync(path.dirname(queuePath), { recursive: true })
  fsImpl.appendFileSync(queuePath, `${JSON.stringify(normalized)}\n`, 'utf8')
  return normalized
}

function normalizeDesktopRemoteAck(value) {
  if (!value || typeof value !== 'object') return null
  const status = safeString(value.status)
  const text = safeString(value.text)
  const requestId = safeString(value.requestId) || safeString(value.request_id) || safeString(value.id)
  if (!status || !requestId) return null
  return {
    channelId: safeString(value.channelId) || safeString(value.channel_id),
    createdAt: Number.isFinite(Number(value.createdAt ?? value.created_at)) ? Number(value.createdAt ?? value.created_at) : Date.now(),
    reason: safeString(value.reason),
    requestId,
    status,
    text
  }
}

function appendDesktopRemoteAck(ackPath, ack, options = {}) {
  const fsImpl = options.fs || fs
  const normalized = normalizeDesktopRemoteAck(ack)
  if (!normalized) {
    throw new Error('desktop remote ack requires requestId and status')
  }
  fsImpl.mkdirSync(path.dirname(ackPath), { recursive: true })
  fsImpl.appendFileSync(ackPath, `${JSON.stringify(normalized)}\n`, 'utf8')
  return normalized
}

function readDesktopRemoteQueue(queuePath, options = {}) {
  const fsImpl = options.fs || fs
  let startOffset = Math.max(0, Number(options.startOffset || 0) || 0)
  let stat
  try {
    stat = fsImpl.statSync(queuePath)
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return { errors: [], events: [], nextOffset: 0 }
    }
    return { errors: [{ error: error?.message || String(error), offset: startOffset }], events: [], nextOffset: startOffset }
  }
  if (stat.size < startOffset) startOffset = 0
  if (stat.size === startOffset) return { errors: [], events: [], nextOffset: startOffset }

  const fd = fsImpl.openSync(queuePath, 'r')
  try {
    const length = stat.size - startOffset
    const buffer = Buffer.alloc(length)
    fsImpl.readSync(fd, buffer, 0, length, startOffset)
    const text = buffer.toString('utf8')
    const events = []
    const errors = []
    let cursor = startOffset
    const lines = text.split('\n')
    const completeLineCount = text.endsWith('\n') ? lines.length - 1 : lines.length - 1
    for (let i = 0; i < completeLineCount; i += 1) {
      const raw = lines[i]
      const lineOffset = cursor
      cursor += Buffer.byteLength(raw + '\n')
      if (!raw.trim()) continue
      try {
        const parsed = JSON.parse(raw)
        const normalized = normalizeDesktopRemoteRequest(parsed)
        if (normalized) events.push(normalized)
        else errors.push({ error: 'invalid-request', offset: lineOffset })
      } catch (error) {
        errors.push({ error: error?.message || String(error), offset: lineOffset })
      }
    }
    return { errors, events, nextOffset: cursor }
  } finally {
    fsImpl.closeSync(fd)
  }
}

function formatDesktopRemoteComposerText(request) {
  const normalized = normalizeDesktopRemoteRequest(request)
  if (!normalized) return ''
  return [
    '[Discord #desktop-chat remote request]',
    `요청자: ${normalized.userName}`,
    normalized.channelId ? `채널: ${normalized.channelId}` : '',
    `요청 ID: ${normalized.id}`,
    '',
    normalized.autoSubmit
      ? '자동 실행 승인됨: Discord #desktop-chat에서 들어온 요청이므로 바로 진행해줘.'
      : '먼저 요청을 검토한 뒤 안전하면 진행해줘.',
    '답변 마지막에는 다음 추천 작업을 1~4 번호 선택지로 제안해줘.',
    '',
    `요청: ${normalized.text}`
  ]
    .filter(Boolean)
    .join('\n')
}

function deliverDesktopRemoteRequest(request, options = {}) {
  const windows = options.windows || []
  let count = 0
  for (const win of windows) {
    try {
      if (!win || win.isDestroyed?.()) continue
      const webContents = win.webContents
      if (!webContents || webContents.isDestroyed?.()) continue
      webContents.send(DESKTOP_REMOTE_EVENT_CHANNEL, request)
      count += 1
    } catch (error) {
      options.log?.(`desktop remote delivery failed: ${error?.message || error}`)
    }
  }
  return count
}

function createDesktopRemoteQueueWatcher(options) {
  const queuePath = options.queuePath || resolveDesktopRemoteQueuePath(options.hermesHome)
  const fsImpl = options.fs || fs
  const pollMs = Math.max(250, Number(options.pollMs || 1500) || 1500)
  const onRequest = typeof options.onRequest === 'function' ? options.onRequest : () => undefined
  const log = typeof options.log === 'function' ? options.log : () => undefined
  let offset = 0
  let timer = null
  let watcher = null
  let closed = false
  let draining = false

  const drain = () => {
    if (closed || draining) return
    draining = true
    try {
      const result = readDesktopRemoteQueue(queuePath, { fs: fsImpl, startOffset: offset })
      offset = result.nextOffset
      for (const err of result.errors) log(`[desktop-remote] skipped queue line at ${err.offset}: ${err.error}`)
      for (const event of result.events) onRequest(event)
    } catch (error) {
      log(`[desktop-remote] queue drain failed: ${error?.message || error}`)
    } finally {
      draining = false
    }
  }

  const start = () => {
    if (closed) return { close }
    try {
      fsImpl.mkdirSync(path.dirname(queuePath), { recursive: true })
    } catch (error) {
      log(`[desktop-remote] could not create queue dir: ${error?.message || error}`)
    }
    drain()
    timer = setInterval(drain, pollMs)
    try {
      watcher = fsImpl.watch(path.dirname(queuePath), (_eventType, filename) => {
        if (!filename || path.basename(String(filename)) === path.basename(queuePath)) {
          setTimeout(drain, 50)
        }
      })
    } catch (error) {
      log(`[desktop-remote] fs.watch unavailable, using poll only: ${error?.message || error}`)
    }
    return { close, drain, queuePath }
  }

  const close = () => {
    closed = true
    if (timer) clearInterval(timer)
    timer = null
    try {
      watcher?.close?.()
    } catch {
      // ignore
    }
    watcher = null
  }

  return { close, drain, queuePath, start }
}

module.exports = {
  DESKTOP_REMOTE_EVENT_CHANNEL,
  appendDesktopRemoteRequest,
  appendDesktopRemoteAck,
  createDesktopRemoteQueueWatcher,
  deliverDesktopRemoteRequest,
  formatDesktopRemoteComposerText,
  normalizeDesktopRemoteRequest,
  normalizeDesktopRemoteAck,
  readDesktopRemoteQueue,
  resolveDesktopRemoteAckPath,
  resolveDesktopRemoteQueuePath
}
