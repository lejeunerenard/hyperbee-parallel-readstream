import test from 'tape'
import { createDB } from './helpers.mjs'

test('parallelReadStream', (t) => {
  t.test('reads all keys', async (t) => {
    const db = await createDB()

    const total = 100

    const awaitAll = []
    for (let i = 0; i < total; i++) {
      awaitAll.push(db.put('key' + i))
    }
    await Promise.all(awaitAll)

    const results = []
    for await (const node of db.parallelReadStream()) {
      results.push(node)
    }

    t.equal(results.length, total)
  })
})
