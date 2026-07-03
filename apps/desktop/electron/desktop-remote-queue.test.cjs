'use strict'

const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const test = require('node:test')

const {
  DESKTOP_REMOTE_EVENT_CHANNEL,
  appendDesktopRemoteRequest,
  deliverDesktopRemoteRequest,
  formatDesktopRemoteComposerText,
  appendDesktopRemoteAck,
  readDesktopRemoteQueue,
  resolveDesktopRemoteAckPath,
  resolveDesktopRemoteQueuePath
} = require('./desktop-remote-queue.cjs')

function mkTmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'hermes-desktop-remote-'))
}

test('resolveDesktopRemoteQueuePath stores the Discord-to-desktop queue under HERMES_HOME/state', () => {
  assert.equal(
    resolveDesktopRemoteQueuePath('/tmp/hermes-home'),
    path.join('/tmp/hermes-home', 'state', 'desktop-remote-requests.jsonl')
  )
})

test('appendDesktopRemoteRequest writes normalized JSONL records and readDesktopRemoteQueue reads only new complete events', () => {
  const root = mkTmpDir()
  try {
    const queuePath = resolveDesktopRemoteQueuePath(root)
    const first = appendDesktopRemoteRequest(queuePath, {
      channelId: '1518889273959518258',
      messageId: 'm1',
      source: 'discord',
      text: '  요약해줘  ',
      userId: 'u1',
      userName: 'Kihoon'
    })

    assert.match(first.id, /^discord-m1-/)
    assert.equal(first.text, '요약해줘')

    fs.appendFileSync(queuePath, '{bad json}\n')
    fs.appendFileSync(queuePath, JSON.stringify({ id: 'blank', text: '   ' }) + '\n')
    const second = appendDesktopRemoteRequest(queuePath, {
      channelId: '1518889273959518258',
      messageId: 'm2',
      source: 'discord',
      text: '다음 추천 작업 알려줘',
      userId: 'u1',
      userName: 'Kihoon'
    })

    const initial = readDesktopRemoteQueue(queuePath, { startOffset: 0 })
    assert.deepEqual(initial.events.map(e => e.id), [first.id, second.id])
    assert.equal(initial.errors.length, 2)

    const next = readDesktopRemoteQueue(queuePath, { startOffset: initial.nextOffset })
    assert.deepEqual(next.events, [])
    assert.equal(next.errors.length, 0)
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('formatDesktopRemoteComposerText makes the inserted Desktop text explicit and safe', () => {
  const text = formatDesktopRemoteComposerText({
    autoSubmit: true,
    channelId: '1518889273959518258',
    id: 'req1',
    source: 'discord',
    text: '현재 상태 요약',
    userName: 'Kihoon'
  })

  assert.match(text, /Discord #desktop-chat remote request/)
  assert.match(text, /요청자: Kihoon/)
  assert.match(text, /현재 상태 요약/)
  assert.match(text, /자동 실행 승인됨/)
})

test('appendDesktopRemoteAck writes normalized busy/fallback acknowledgements under HERMES_HOME/state', () => {
  const root = mkTmpDir()
  try {
    const ackPath = resolveDesktopRemoteAckPath(root)
    const ack = appendDesktopRemoteAck(ackPath, {
      channelId: '1518889273959518258',
      requestId: 'req1',
      status: 'busy',
      text: '작업 본문',
      reason: 'desktopRemoteSubmitting'
    })

    assert.equal(ack.requestId, 'req1')
    assert.equal(ack.status, 'busy')
    assert.equal(ack.text, '작업 본문')
    assert.equal(ack.channelId, '1518889273959518258')
    const lines = fs.readFileSync(ackPath, 'utf8').trim().split('\n')
    assert.equal(lines.length, 1)
    assert.deepEqual(JSON.parse(lines[0]), ack)
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('deliverDesktopRemoteRequest sends to all live Desktop windows on the IPC channel', () => {
  const sent = []
  const windows = [
    { isDestroyed: () => false, webContents: { isDestroyed: () => false, send: (channel, payload) => sent.push({ channel, payload }) } },
    { isDestroyed: () => true, webContents: { isDestroyed: () => false, send: () => sent.push('bad') } },
    { isDestroyed: () => false, webContents: { isDestroyed: () => true, send: () => sent.push('bad') } }
  ]

  const delivered = deliverDesktopRemoteRequest({ id: 'req1', text: 'hello' }, { windows })

  assert.equal(delivered, 1)
  assert.deepEqual(sent, [{ channel: DESKTOP_REMOTE_EVENT_CHANNEL, payload: { id: 'req1', text: 'hello' } }])
})
