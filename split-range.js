import b4a from 'b4a'
import debug from 'debug'
import { bytewiseSubtract, bytewiseDivide, bytewiseAddition } from './key-math.js'

const d = {
  results: debug('split-range:results'),
  treeTransversal: debug('split-range:tree-transversal')
}

const SEP = b4a.alloc(1)
const EMPTY = b4a.alloc(0)

export async function trimKeySpace (db, range) {
  const _gKey = range.gte || range.gt
  const _lKey = range.lte || range.lt
  // TODO See if there is something in hyperbee I can piggy back on for encoding
  const userStart = _gKey ? db.keyEncoding.encode(_gKey) : null
  const userEnd = _lKey ? db.keyEncoding.encode(_lKey) : null

  const keyspaceStartNode = await db.peek({ ...range, reverse: false })
  const keyspaceStart = keyspaceStartNode ? b4a.from(keyspaceStartNode.key) : null

  const keyspaceEndNode = await db.peek({ ...range, reverse: true })
  const keyspaceEnd = keyspaceEndNode ? b4a.from(keyspaceEndNode.key) : null

  const start = userStart && userStart.compare(keyspaceStart) > 0 ? userStart : keyspaceStart
  const end = !userEnd || userEnd.compare(keyspaceEnd) > 0 ? keyspaceEnd : userEnd

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

export async function * getNextKeyFromBTree (db, range, targetNumber) {
  const trimed = await trimKeySpace(db, range)
  const keys = await splitKeysFromBTree(db, trimed, targetNumber)
  d.results('keys length', keys.length)
  const carry = Buffer.allocUnsafe(keys[0].byteLength).fill(0)
  keys[0].copy(carry)
  keys.push(trimed.lte)
  for (let i = 0; i < keys.length; i++) {
    yield keys[i]
  }
}

async function splitKeysFromBTree (db, range, targetNumber) {
  // Use b-tree for divvying ranges
  const batch = db.batch()
  const root = await batch.getRoot(false)
  const [totalMaxDepth, stack] = await getKeysFromTree(root, encRange(db.keyEncoding, range), targetNumber)

  d.results('initial unsorted stack', stack, 'length', stack.length)

  if (b4a.compare(range.gte, stack[0]) < 0) {
    d.results('adding gte')
    stack.unshift(range.gte)
  }

  if (b4a.compare(range.lte, stack[stack.length - 1]) > 0) {
    d.results('adding lte')
    stack.push(range.lte)
  }

  d.results('totalMaxDepth', totalMaxDepth)

  const sorted = stack.sort(b4a.compare)
  return sorted
}

// Recurse following valid children, accumlating keys until roughly the initial
// `numberRemaining` is met or exceeded
export async function getKeysFromTree (node, range, numberRemaining) {
  const _lKey = range.lte || null
  const _gKey = range.gte || null

  d.treeTransversal('lowerbound', _gKey, 'upperbound', _lKey)
  d.treeTransversal('node\'s # of keys', node.keys.length)

  let c
  let maxDepth = 0

  const keys = []
  const pursueable = []

  let greaterThanOrEqualToUpperBound = false
  for (let i = 0; i < node.keys.length; i++) {
    const key = (await node.getKey(i))
    d.treeTransversal('checking key', key)
    c = _gKey ? b4a.compare(_gKey, key) : -1
    if (c === 0) {
      // Found starting key
      keys.push(_gKey)
    } else if (c < 0) {
      c = _lKey ? b4a.compare(_lKey, key) : 1
      if (c === 0) {
        greaterThanOrEqualToUpperBound = true
        // Found ending key
        keys.push(key)
        break
      } else if (c < 0) {
        // Found a key out of range
        // Mark correspond child for perusal, but flag out of bounds
        greaterThanOrEqualToUpperBound = true
        pursueable.push(i)
        break
      } else {
        // Found a key in between
        keys.push(key)
        if (i < node.children.length) {
          pursueable.push(i)
        }
      }
    } else {
      d.treeTransversal('found key below starting')
      // TODO
      continue
    }
  }

  // All keys must be lower than upper bound, so include last (highest) child
  if (!greaterThanOrEqualToUpperBound && node.children.length) {
    pursueable.push(node.children.length - 1)
  }

  if (keys.length) {
    maxDepth = 1
  }

  // Pursue
  d.treeTransversal('keys.length < numberRemaining', keys.length < numberRemaining, 'keys.length', keys.length, 'numberRemaining', numberRemaining)
  if (keys.length < numberRemaining) {
    const numberOfKeysPerChild = (numberRemaining - keys.length) / pursueable.length
    let maxChildDepth = 0
    d.treeTransversal('pursueable.length', pursueable.length)
    d.treeTransversal('children.length', node.children.length)
    for (const childIndex of pursueable) {
      d.treeTransversal('checking child', childIndex)
      const child = await node.getChildNode(childIndex)
      const [childMaxDepth, childKeys] = await getKeysFromTree(child, range, numberOfKeysPerChild)
      maxChildDepth = Math.max(maxChildDepth, childMaxDepth)
      keys.push(...childKeys)
    }

    maxDepth += maxChildDepth
  }

  d.treeTransversal('maxDepth', maxDepth, 'numberRemaining', numberRemaining)
  return [maxDepth, keys]
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
