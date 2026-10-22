import { expect, test } from "bun:test"
import { make } from "../src/promise/generated/client"

const limit = 16 * 1024 * 1024
const chunks = [Infinity, 64 * 1024]

test.each(chunks)("SSE accepts separate sub-limit events in %s-byte chunks", async (chunkSize) => {
  const events = [
    { type: "session.reasoning.ended", data: { sessionID: "ses_one", text: "x".repeat(8 * 1024 * 1024) } },
    { type: "session.reasoning.ended", data: { sessionID: "ses_two", text: "y".repeat(8 * 1024 * 1024) } },
  ]
  const payload = events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join("")
  expect(payload.length).toBeGreaterThan(limit)
  const stream = subscription(payload, chunkSize)

  for (const event of events) await expect(stream.iterator.next()).resolves.toEqual({ done: false, value: event })
  await expect(stream.iterator.next()).resolves.toMatchObject({ done: true })
})

test.each(chunks)(
  "SSE rejects an oversized complete event in %s-byte chunks after delivering prior events",
  async (chunkSize) => {
    const event = { type: "server.connected", data: {} }
    const stream = subscription(
      `data: ${JSON.stringify(event)}\n\n${frame("x".repeat(limit))}${frame("marker")}`,
      chunkSize,
      false,
    )

    await expect(stream.iterator.next()).resolves.toEqual({ done: false, value: event })
    await expect(stream.iterator.next()).rejects.toMatchObject({ name: "ClientError", reason: "SseEventTooLarge" })
    expect(stream.cancelled()).toBe(true)
    await expect(stream.iterator.next()).resolves.toMatchObject({ done: true })
  },
)

test.each(chunks)("SSE bounds an unfinished event without waiting for EOF in %s-byte chunks", async (chunkSize) => {
  const stream = subscription(frame("x".repeat(limit)).slice(0, -2), chunkSize, false)

  await expect(stream.iterator.next()).rejects.toMatchObject({ name: "ClientError", reason: "SseEventTooLarge" })
  expect(stream.cancelled()).toBe(true)
  await expect(stream.iterator.next()).resolves.toMatchObject({ done: true })
})

test.each(chunks)("SSE retains its decoded UTF-16 budget for multibyte text in %s-byte chunks", async (chunkSize) => {
  const text = "雪".repeat(6 * 1024 * 1024)
  const payload = frame(text)
  expect(payload.length).toBeLessThan(limit)
  expect(new TextEncoder().encode(payload).length).toBeGreaterThan(limit)
  const stream = subscription(payload, chunkSize)

  await expect(stream.iterator.next()).resolves.toMatchObject({ done: false, value: { data: { text } } })
  await expect(stream.iterator.next()).resolves.toMatchObject({ done: true })
})

test("SSE counts astral characters as two UTF-16 code units", async () => {
  const stream = subscription(frame("😀".repeat(limit / 2)), Infinity, false)

  await expect(stream.iterator.next()).rejects.toMatchObject({ name: "ClientError", reason: "SseEventTooLarge" })
  expect(stream.cancelled()).toBe(true)
})

test.each(["\n\n", "\r\n\r\n", ""])(
  "SSE accepts a frame exactly at the limit with terminator %j across chunks",
  async (terminator) => {
    const text = "x".repeat(limit - frame("").length + (terminator === "" ? 2 : 0))
    const payload = frame(text).slice(0, -2) + terminator
    for (const chunkSize of [Infinity, new TextEncoder().encode(payload).length - 1]) {
      const stream = subscription(payload, chunkSize)
      await expect(stream.iterator.next()).resolves.toMatchObject({ done: false, value: { data: { text } } })
      await expect(stream.iterator.next()).resolves.toMatchObject({ done: true })
    }
  },
)

test.each(chunks)("SSE rejects a frame one code unit above the limit in %s-byte chunks", async (chunkSize) => {
  const stream = subscription(frame("x".repeat(limit - frame("").length + 1)), chunkSize, false)

  await expect(stream.iterator.next()).rejects.toMatchObject({ name: "ClientError", reason: "SseEventTooLarge" })
  expect(stream.cancelled()).toBe(true)
})

function frame(text: string) {
  return `data: ${JSON.stringify({ type: "session.reasoning.ended", data: { sessionID: "ses_test", text } })}\n\n`
}

function subscription(payload: string, chunkSize: number, close = true) {
  const bytes = new TextEncoder().encode(payload)
  let offset = 0
  let cancelled = false
  const client = make({
    baseUrl: "http://localhost:3000",
    fetch: async () =>
      new Response(
        new ReadableStream<Uint8Array>({
          pull(controller) {
            if (offset === bytes.length) {
              if (close) controller.close()
              return
            }
            controller.enqueue(bytes.subarray(offset, offset + chunkSize))
            offset = Math.min(offset + chunkSize, bytes.length)
          },
          cancel() {
            cancelled = true
          },
        }),
        { headers: { "content-type": "text/event-stream" } },
      ),
  })
  return { iterator: client.event.subscribe()[Symbol.asyncIterator](), cancelled: () => cancelled }
}
