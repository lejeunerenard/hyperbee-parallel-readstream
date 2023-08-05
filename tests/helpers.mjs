import Hypercore from 'hypercore'
import HyperbeeParallel from '../index.js'
import { mkdtemp } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

export async function createDB () {
  const dir = await mkdtemp(join(tmpdir(), 'hb-p-rs'))
  const core = new Hypercore(dir)
  const db = new HyperbeeParallel(core, { keyEncoding: 'utf-8' })
  await db.ready()
  return db
}
