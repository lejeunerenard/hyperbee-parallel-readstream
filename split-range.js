import b4a from 'b4a'
import { bytewiseSubtract, bytewiseDivide, bytewiseAddition } from './key-math.js'

const SEP = b4a.alloc(1)
const EMPTY = b4a.alloc(0)

async function trimKeySpace (db, range) {
  // TODO See if there is something in hyperbee I can piggy back on for encoding
  const userStart = db.keyEncoding.encode(range.gte || range.gt)
  const userEnd = db.keyEncoding.encode(range.lte || range.lt)

  const { key: keyspaceStartStr } = await db.peek({ ...range, reverse: false })
  const { key: keyspaceEndStr } = await db.peek({ ...range, reverse: true })
  const keyspaceStart = b4a.from(keyspaceStartStr)
  const keyspaceEnd = b4a.from(keyspaceEndStr)

  const start = userStart.compare(keyspaceStart) > 0 ? userStart : keyspaceStart
  const end = userEnd.compare(keyspaceEnd) > 0 ? keyspaceEnd : userEnd

  const trimmedRange = { ...range, gte: start, lte: end }
  delete trimmedRange.lt
  delete trimmedRange.gt

  return trimmedRange
}

export async function * getNextKeyBySplitting (db, range, targetNumber) {
  const trimedRange = await trimKeySpace(db, range)
  const diff = bytewiseSubtract(trimedRange.gte, trimedRange.lte)
  const inc = bytewiseDivide(diff, targetNumber)

  let carry = Buffer.allocUnsafe(trimedRange.gte.byteLength).fill(0)
  trimedRange.gte.copy(carry)
  yield carry
  for (let i = 0; i < targetNumber; i++) {
    const higher = bytewiseAddition(inc, carry)
    yield higher
    carry = higher
  }
}

// TODO Walk the rest of the tree, currently only walks to the 'starting' node
export async function * getNextKeyFromBTree (db, range, targetNumber) {
  const trimed = await trimKeySpace(db, range)
  const keys = await splitKeysFromBTree(db, trimed)
  const carry = Buffer.allocUnsafe(keys[0].byteLength).fill(0)
  keys[0].copy(carry)
  keys.push(trimed.lte)
  for (let i = 1; i < keys.length; i++) {
    yield keys[i]
  }
}

// async function renderKeys (treeNode) {
//   for (let i = 0; i < treeNode.keys.length; i++) {
//     console.log('i', (await treeNode.getKey(i)).toString())
//   }
//   console.log('treeNode.children.length', treeNode.children.length)
// }

export async function splitKeysFromBTree (db, range) {
  // Use b-tree for divvying ranges
  const batch = db.batch()
  const root = await batch.getRoot(false)
  const stack = await getKeysFromTree(root, encRange(db.keyEncoding, range))

  return stack.flatMap(({ node }) => node.keys
    .filter((key) => !!key.value).map((key) => key.value))
    .sort(b4a.compare)
}

// Might need a different approach as this walks to the 'start' node. A better
// approach might be fanning out from the root until including a) enough nodes
// for the number of threads to be run, b) including the entire range.
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
