import Hyperbee from 'hyperbee'
// import Corestore from 'corestore'
import Hypercore from 'hypercore'
import b4a from 'b4a'
// import util from 'util'
import equal from 'fast-deep-equal'
import { bytewiseSubtract, bytewiseDivide, bytewiseAddition } from './key-math.js'

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

// async function renderKeys (treeNode) {
//   for (let i = 0; i < treeNode.keys.length; i++) {
//     console.log('i', (await treeNode.getKey(i)).toString())
//   }
//   console.log('treeNode.children.length', treeNode.children.length)
// }

async function getKeysFromTree (node, range) {
  const _reverse = !!range.reverse
  const _lIncl = !range.lt
  const _gIncl = !range.gt
  const _lKey = range.lt || range.lte || null
  const _gKey = range.gt || range.gte || null

  const incl = _reverse ? _lIncl : _gIncl
  const start = _reverse ? _lKey : _gKey
  const stack = []

  if (!start) {
    stack.push({ node, i: _reverse ? node.keys.length << 1 : 0 })
    return stack
  }

  while (true) {
    const entry = { node, i: _reverse ? node.keys.length << 1 : 0 }

    let s = 0
    let e = node.keys.length
    let c

    while (s < e) {
      const mid = (s + e) >> 1
      c = b4a.compare(start, await node.getKey(mid))

      if (c === 0) {
        if (incl) entry.i = mid * 2 + 1
        else entry.i = mid * 2 + (_reverse ? 0 : 2)
        stack.push(entry)
        return stack
      }

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    entry.i = 2 * i + (_reverse ? -1 : 1)

    if (entry.i >= 0 && entry.i <= (node.keys.length << 1)) stack.push(entry)
    if (!node.children.length) {
      return stack
    }

    node = await node.getChildNode(i)
  }
}

async function parallelReadStream (directory, db, range) {
  const version = db.version
  const numRun = pool.numThreads
  const results = []
  // TODO See if there is something in hyperbee I can piggy back on for encoding
  const userStart = db.keyEncoding.encode(range.gte || range.gt)
  const userEnd = db.keyEncoding.encode(range.lte || range.lt)

  const snapshot = db.checkout(version)

  // Use b-tree for divvying ranges
  const batch = snapshot.batch()
  const root = await batch.getRoot(false)
  const stack = await getKeysFromTree(root, encRange(db.keyEncoding, range))

  // console.log('stack', stack)
  // for (const { node } of stack) {
  //   console.log('node.children', node.children)
  //   for (let i = 0; i < node.children.length; i++) {
  //     console.log(await node.getChildNode(i))
  //   }
  // }

  const keys = stack.flatMap(({ node }) => node.keys
    .filter((key) => !!key.value).map((key) => key.value))
    .sort(b4a.compare)

  // console.log('keys', keys)
  // console.log('keys.length', keys.length)

  const { key: keyspaceStartStr } = await snapshot.peek({ ...range, reverse: false })
  const { key: keyspaceEndStr } = await snapshot.peek({ ...range, reverse: true })
  const keyspaceStart = b4a.from(keyspaceStartStr)
  const keyspaceEnd = b4a.from(keyspaceEndStr)
  const start = userStart.compare(keyspaceStart) > 0 ? userStart : keyspaceStart
  const end = userEnd.compare(keyspaceEnd) > 0 ? keyspaceEnd : userEnd

  console.log('numRun', numRun)
  console.log('userStart', userStart, 'userEnd', userEnd)
  console.log('keyspaceStart', keyspaceStart, 'keyspaceEnd', keyspaceEnd)
  console.log('userStart.compare(keyspaceStart)', userStart.compare(keyspaceStart))
  console.log('userEnd.compare(keyspaceEnd)', userEnd.compare(keyspaceEnd))
  console.log('start', start, 'end', end)

  // const diff = lexicographicMidPoint(start, end)
  const diff = bytewiseSubtract(start, end)
  console.log('diff', diff)
  const inc = bytewiseDivide(diff, numRun)
  console.log('inc', inc)

  // 0 is increment
  // // 1 is b-tree based
  const KEY_MODE = 0

  let carry
  if (KEY_MODE === 0) {
    carry = Buffer.allocUnsafe(start.byteLength).fill(0)
    start.copy(carry)
  } else {
    carry = Buffer.allocUnsafe(keys[0].byteLength).fill(0)
    keys[0].copy(carry)
    keys.push(end)
  }

  const prevLt = b4a.allocUnsafe(carry.byteLength).fill(0)
  carry.copy(prevLt)

  let foundStart = false
  let foundEnd = false

  let finished = 0
  let finishedTarget = finished
  return new Promise((resolve, reject) => {
    finishedTarget = numRun
    for (let i = 0; i < numRun; i++) {
      const higher = bytewiseAddition(inc, carry)

      // finishedTarget = keys.length - 1
      // for (let i = 1; i < keys.length; i++) {
      //   const higher = keys[i]

      const rangeSplit = { ...range }
      delete rangeSplit.lt
      delete rangeSplit.gt
      delete rangeSplit.gte
      const gtX = i === 0 ? 'gte' : 'gt'
      rangeSplit[gtX] = carry
      rangeSplit.lte = higher

      console.log('rangeSplit', rangeSplit)
      if (rangeSplit[gtX].compare(rangeSplit.lte) > 0) throw Error('wrong order')

      if (rangeSplit[gtX].compare(start) === 0) foundStart = true
      if (rangeSplit.lte.compare(end) === 0) foundEnd = true

      pool.runTask({
        dbOpts: {
          keyEncoding: 'utf-8',
          valueEncoding: 'json'
        },
        version,
        directory,
        range: rangeSplit
      }, (err, msg) => {
        console.log(i, err, msg ? msg.length : null)
        if (!err) results.push(msg)
        if (++finished === finishedTarget) {
          resolve(results.flat())
        }
      })

      // resolve()

      // higher.copy(carry)
      carry = higher
    }

    console.log('end', end)
    console.log('foundStart', foundStart)
    console.log('foundEnd', foundEnd)
    if (!(foundStart && foundEnd)) {
      throw Error('not covering')
    }
  })
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

const SEP = b4a.alloc(1)
const EMPTY = b4a.alloc(0)

function encRange (e, opts) {
  if (!e) return opts

  if (e.encodeRange) {
    const r = e.encodeRange({ gt: opts.gt, gte: opts.gte, lt: opts.lt, lte: opts.lte })
    opts.gt = r.gt
    opts.gte = r.gte
    opts.lt = r.lt
    opts.lte = r.lte
    return opts
  }

  if (opts.gt !== undefined) opts.gt = enc(e, opts.gt)
  if (opts.gte !== undefined) opts.gte = enc(e, opts.gte)
  if (opts.lt !== undefined) opts.lt = enc(e, opts.lt)
  if (opts.lte !== undefined) opts.lte = enc(e, opts.lte)
  if (opts.sub && !opts.gt && !opts.gte) opts.gt = enc(e, SEP)
  if (opts.sub && !opts.lt && !opts.lte) opts.lt = bump(enc(e, EMPTY))

  return opts
}

function bump (key) {
  // key should have been copied by enc above before hitting this
  key[key.length - 1]++
  return key
}

function enc (e, v) {
  if (v === undefined || v === null) return null
  if (e !== null) return e.encode(v)
  if (typeof v === 'string') return b4a.from(v)
  return v
}
