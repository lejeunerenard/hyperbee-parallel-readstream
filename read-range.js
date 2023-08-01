import { parentPort } from 'node:worker_threads'
import Hyperbee from 'hyperbee'
import Hypercore from 'hypercore'
import b4a from 'b4a'

parentPort.on('message', async ({ dbOpts, version, directory, range }) => {
  const core = new Hypercore(directory, { unlocked: true, writable: false })
  const db = new Hyperbee(core, dbOpts)
  await db.ready()
  const snap = db.checkout(version)

  if ('lt' in range) range.lt = b4a.from(range.lt)
  if ('lte' in range) range.lte = b4a.from(range.lte)
  if ('gt' in range) range.gt = b4a.from(range.gt)
  if ('gte' in range) range.gte = b4a.from(range.gte)

  // console.log('range', range)
  for await (const node of snap.createReadStream(range)) {
    parentPort.postMessage({ event: 'node', node })
  }

  parentPort.postMessage({ event: 'done' })
})
