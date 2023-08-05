import test from 'tape'
import b4a from 'b4a'
import { trimKeySpace, getKeysFromTree } from '../split-range.js'
import { createDB } from './helpers.mjs'

const putInOrder = async (db, keys) =>
  keys.reduce((accum, k) =>
    accum.then(() => db.put(k)),
  Promise.resolve())

test('split-range', (t) => {
  t.test('trimKeySpace', (t) => {
    t.test('undefined bounds return null', async (t) => {
      // Construct b-tree
      const db = await createDB()
      const trimmedRange = await trimKeySpace(db, {})
      t.equal(trimmedRange.lte, null)
      t.equal(trimmedRange.gte, null)
    })
  })

  t.test('gets only lower bound key when its the highest key', async (t) => {
    // Construct b-tree
    const db = await createDB()

    const gteKey = 'key5'
    const range = { gte: b4a.from(gteKey), lt: b4a.from('keyzz') }
    await db.put(gteKey)
    await putInOrder(db, [
      'key0',
      'key1',
      'key2',
      gteKey
    ])

    const batch = db.batch()
    const root = await batch.getRoot(false)

    // getKeysFromTree requires defined & trimmed key space
    const trimmedRange = await trimKeySpace(db, range)
    const [, keys] = await getKeysFromTree(root, trimmedRange, 4)
    t.deepEqual(keys, [b4a.from(gteKey)], 'found lower bound')
  })

  t.test('transverses to child nodes even when parent node\'s key is below lower bound', async (t) => {
    // Construct b-tree
    const db = await createDB()

    const gteKey = 'key5'
    const range = { gte: b4a.from(gteKey), lt: b4a.from('keyzz') }
    await putInOrder(db, [
      'kexd',
      'kexe',
      'kexf',
      'key0',
      'key1',
      'key2',
      'key3',
      'key4',
      // matching
      'key6',
      'key7'
    ])

    const batch = db.batch()
    const root = await batch.getRoot(false)

    // getKeysFromTree requires defined & trimmed key space
    const trimmedRange = await trimKeySpace(db, range)
    const [, keys] = await getKeysFromTree(root, trimmedRange, 4)
    t.deepEqual(keys, [b4a.from('key6'), b4a.from('key7')], 'found expected keys')
  })

  t.test('doesnt assume finding a key in bound means there is a child node', async (t) => {
    // Construct b-tree
    const db = await createDB()

    const gteKey = 'key5'
    const range = { gte: b4a.from(gteKey), lt: b4a.from('keyzz') }
    await putInOrder(db, [
      'kexe',
      'kexf',
      'key0',
      'key1',
      'key2',
      'key3',
      'key4',
      // matching
      'key5',
      'key6',
      'key7'
    ])

    const batch = db.batch()
    const root = await batch.getRoot(false)

    // getKeysFromTree requires defined & trimmed key space
    const trimmedRange = await trimKeySpace(db, range)
    const [, keys] = await getKeysFromTree(root, trimmedRange, 4)
    t.deepEqual(keys, [b4a.from('key5'), b4a.from('key6'), b4a.from('key7')], 'found expected keys')
  })

  t.test('doesnt assume finding a key in bound means there is a child node', async (t) => {
    // Construct b-tree
    const db = await createDB()

    const gteKey = 'key5'
    const range = { gte: b4a.from(gteKey), lt: b4a.from('keyzz') }
    await db.put('kexe')
    await db.put('kexf')
    await db.put('key0')
    await db.put('key1')
    await db.put('key2')
    await db.put('key3')
    await db.put('key4')
    // matching
    await db.put('key5')
    await db.put('key6')
    await db.put('key7')

    const batch = db.batch()
    const root = await batch.getRoot(false)

    // getKeysFromTree requires defined & trimmed key space
    const trimmedRange = await trimKeySpace(db, range)
    const [, keys] = await getKeysFromTree(root, trimmedRange, 4)
    t.deepEqual(keys, [b4a.from('key5'), b4a.from('key6'), b4a.from('key7')], 'found expected keys')
  })

  t.test('finds children keys when parent key is in bounds', async (t) => {
    // Construct b-tree
    const db = await createDB()

    const gteKey = 'key0'
    const range = { gte: b4a.from(gteKey), lt: b4a.from('keyzz') }
    await db.put('kexe')
    await db.put('kexf')
    await db.put('key0')
    await db.put('key1')
    await db.put('key2') // <- key of first split
    await db.put('key3')
    await db.put('key4')
    // matching
    await db.put('key5')
    await db.put('key6')
    await db.put('key7')

    const batch = db.batch()
    const root = await batch.getRoot(false)

    // getKeysFromTree requires defined & trimmed key space
    const trimmedRange = await trimKeySpace(db, range)
    const [, keys] = await getKeysFromTree(root, trimmedRange, 4)
    t.deepEqual(keys, [
      b4a.from('key2'), // first split key
      b4a.from('key0'), // first child keys
      b4a.from('key1'), // first child keys
      b4a.from('key3'), // second child keys
      b4a.from('key4'), // second child keys
      b4a.from('key5'), // second child keys
      b4a.from('key6'), // second child keys
      b4a.from('key7') // second child keys
    ], 'found expected keys')
  })

  t.test('finds keys w/ empty range', async (t) => {
    // Construct b-tree
    const db = await createDB()

    const range = {}
    await db.put('kexe')
    await db.put('kexf')
    await db.put('key0')
    await db.put('key1')
    await db.put('key2') // <- key of first split
    await db.put('key3')
    await db.put('key4')
    await db.put('key5')
    await db.put('key6')
    await db.put('key7')

    const batch = db.batch()
    const root = await batch.getRoot(false)

    // getKeysFromTree requires defined & trimmed key space
    const trimmedRange = await trimKeySpace(db, range)
    const [, keys] = await getKeysFromTree(root, trimmedRange, 4)
    t.deepEqual(keys, [
      b4a.from('key2'), // first split key
      b4a.from('kexe'), // first child keys
      b4a.from('kexf'), // first child keys
      b4a.from('key0'), // first child keys
      b4a.from('key1'), // first child keys
      b4a.from('key3'), // second child keys
      b4a.from('key4'), // second child keys
      b4a.from('key5'), // second child keys
      b4a.from('key6'), // second child keys
      b4a.from('key7') // second child keys
    ], 'found expected keys')
  })
})
