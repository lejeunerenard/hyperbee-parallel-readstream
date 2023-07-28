import equal from 'fast-deep-equal'
import HyperbeeParallel from './index.js'
import { pack } from 'lexicographic-integer'
import Corestore from 'corestore'

const KEY_SPACE_STYLE = 1 // 0 = numbers as strings (more even) 1 = lexicographic number ordering
const COMPLEX_VALUES = true

const storeDir = KEY_SPACE_STYLE === 0 ? 'store-string-number' : 'store-lexicographic-number'
const store = new Corestore(storeDir + (COMPLEX_VALUES ? '-complex' : ''))
const core = store.get({ name: 'bee' })
await core.ready()

const db = new HyperbeeParallel(core, { keyEncoding: 'utf-8', valueEncoding: 'json' })

await db.ready()
console.log('db.version', db.version)

const INIT = db.version === 1
if (INIT) {
  for (let i = 0; i < 1_000_000; i++) {
    const key = 'key' + (KEY_SPACE_STYLE === 0 ? i : pack(i, 'hex'))
    const value = COMPLEX_VALUES ? { i, foo: { bar: 10, biz: true, baz: { b: { c: { word: '12038arsitenadrlyahrstokarstiehastjkl' } } } } } : { i }
    await db.put(key, value)
    if (i % 10_000 === 0) {
      console.log('i', i, 'key', key)
    }
  }
}

const range = { lt: 'keyg', gte: 'key' }

console.time('read-parallel')
const nodes = await db.parallelReadStream(range)
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
