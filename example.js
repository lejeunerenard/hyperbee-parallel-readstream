import Hypercore from 'hypercore'
import equal from 'fast-deep-equal'
import { HyperbeeParallel } from './index.js'
import { pack } from 'lexicographic-integer'

const core = new Hypercore('./bee')

// // TODO Test with corestore version
// const core = store.get({ name: 'bee' })
const db = new HyperbeeParallel(core, { keyEncoding: 'utf-8', valueEncoding: 'json' })

await db.ready()
console.log('db.version', db.version)

const INIT = db.version === 1
if (INIT) {
  for (let i = 0; i < 1_000_000; i++) {
    const key = 'key' + pack(i, 'hex')
    await db.put(key, { i })
    if (i % 10_000 === 0) {
      console.log('i', i)
    }
  }
}

const range = { lt: 'keyg', gte: 'key' }

db.put('should locked?')
console.time('read-parallel')
const readPromise = db.parallelReadStream(range)
db.put('unlocked?')
console.log('after unlocked')
const nodes = await readPromise
console.timeEnd('read-parallel')
console.log('nodes.length', nodes.length)

console.time('read-sequential')
const nodesSquential = []
for await (const node of db.createReadStream(range)) {
  nodesSquential.push(node)
}
console.timeEnd('read-sequential')
console.log('nodesSquential.length', nodesSquential.length)

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
