import Hyperbee from 'hyperbee'
import Hypercore from 'hypercore'
import equal from 'fast-deep-equal'
import { getNextKeyBySplitting } from './split-range.js'

import WorkerPool from './worker-pool.js'
import os from 'node:os'

const directory = './bee'
const core = new Hypercore(directory, { unlocked: true })

// // TODO Test with corestore version
// const core = store.get({ name: 'bee' })
const db = new Hyperbee(core, { keyEncoding: 'utf-8', valueEncoding: 'json' })

await db.ready()
console.log('db.version', db.version)
console.log('core.writable', core.writable)

const INIT = db.version === 1
if (INIT) {
  for (let i = 0; i < 1_000_000; i++) {
    await db.put('key' + i, { i })
    if (i % 10_000 === 0) {
      console.log('i', i)
    }
  }
}

const pool = new WorkerPool(os.cpus().length)

async function parallelReadStream (directory, db, range) {
  const version = db.version
  const numRun = pool.numThreads

  const snapshot = db.checkout(version)

  const keyIter = getNextKeyBySplitting(snapshot, range, numRun)
  let carry = (await keyIter.next()).value

  const tasks = []
  let firstRange = true
  for await (const higher of keyIter) {
    const rangeSplit = { ...range }
    delete rangeSplit.lt
    delete rangeSplit.gt
    delete rangeSplit.gte

    const gtX = firstRange ? 'gte' : 'gt'
    firstRange = false
    rangeSplit[gtX] = carry
    rangeSplit.lte = higher

    if (rangeSplit[gtX].compare(rangeSplit.lte) > 0) throw Error('wrong order')

    tasks.push(new Promise((resolve, reject) => {
      pool.runTask({
        dbOpts: {
          keyEncoding: 'utf-8',
          valueEncoding: 'json'
        },
        version,
        directory,
        range: rangeSplit
      }, (err, msg) => {
        if (!err) return resolve(msg)
        reject(err)
      })
    }))

    carry = higher
  }

  return Promise.all(tasks).then((results) => results.flat())
}

const range = { lt: 'keya', gte: 'key' }

console.time('read-parallel')
const nodes = await parallelReadStream(directory, db, range)
console.timeEnd('read-parallel')
console.log('nodes.length', nodes.length)

console.time('read-sequential')
const nodesSquential = []
for await (const node of db.createReadStream(range)) {
  nodesSquential.push(node)
}
console.timeEnd('read-sequential')
console.log('nodesSquential.length', nodesSquential.length)

// if (nodes.length !== nodesSquential.length) {
const nodesSorted = nodes.sort((a, b) => a.seq - b.seq)
const nodesSquentialSorted = nodesSquential.sort((a, b) => a.seq - b.seq)

for (let i = 0; i < nodesSquentialSorted.length; i++) {
  if (!equal(nodesSorted[i], nodesSquentialSorted[i])) {
    console.log('i', i)
    console.log('nodesSorted[i]', nodesSorted[i])
    console.log('nodesSquentialSorted[i]', nodesSquentialSorted[i])
    throw Error('mismatch')
  }
}
// }

pool.close()
