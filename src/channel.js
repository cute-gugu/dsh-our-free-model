/**
 * A single-producer / single-consumer channel bridging a callback source (the
 * SSE reader hands us one payload per invocation) to an async iterable consumer
 * (the chunk parser).
 *
 * `push` never blocks the producer: unclaimed values queue, which is safe here
 * because a provider stream is bounded by its own turn. `next` suspends the
 * consumer instead of polling, so an idle stream costs nothing.
 *
 * The queue is not unbounded, though: if the consumer stops iterating while the
 * upstream keeps producing, payloads used to pile up until the turn's timeout —
 * tens of MB on a chatty stream. Past {@link CHANNEL_LIMIT} pending values the
 * channel fails itself with an Error, which surfaces to the consumer on the
 * next read while the adapter's abort path tears the upstream down.
 *
 * @module src/channel.js
 */

const CHANNEL_LIMIT = 4096

export function createChannel() {
  /** @type {Array<string | undefined | Error>} */
  const queue = []
  /** @type {Array<(v: string | undefined | Error) => void>} */
  const waiting = []
  let ended = false

  return {
    /** @param {string | undefined | Error} value - payload; `undefined` ends; `Error` fails */
    push(value) {
      if (ended) return
      const resolve = waiting.shift()
      if (resolve !== undefined) resolve(value)
      else if (queue.length >= CHANNEL_LIMIT && value !== undefined && !(value instanceof Error)) {
        // Only regular payloads trip the cap: a terminal value must always get
        // through, or a stream ending exactly at the limit would end as a stall.
        ended = true
        queue.push(new Error('our-free-model: stream consumer stalled; dropping the upstream'))
        return
      } else queue.push(value)
      if (value === undefined || value instanceof Error) ended = true
    },
    async * read() {
      while (true) {
        const next = queue.length > 0
          ? queue.shift()
          : await new Promise(resolve => waiting.push(resolve))
        if (next instanceof Error) throw next
        if (next === undefined) return
        yield next
      }
    },
  }
}
