import Hyperbee from 'hyperbee'
import RAF from 'random-access-file'
import { getNextKeyFromBTree } from './split-range.js'

import WorkerPool from './worker-pool.js'
import os from 'node:os'

export class HyperbeeParallel extends Hyperbee {
  constructor (core, opts = {}) {
    super(core, opts)
    this.originalOpts = opts

    this.numThreads = opts.numThreads || os.cpus().length

    // Check storage
    if (this.core.storage instanceof Function) {
      const fakeDirStorage = this.core.storage('') // blank string

      if (!(fakeDirStorage instanceof RAF)) {
        throw Error('HyperbeeParallel requires it\'s hypercore have a RandomAccessFile type storage')
      }

      // TODO Determine if the trailing `/` is ever a `\` for windows
      this.directory = fakeDirStorage.filename.replace(/\/$/, '')
    }
  }

  async parallelReadStream (range) {
    const pool = new WorkerPool(this.numThreads)

    const version = this.version
    const numRun = this.numThreads

    const snapshot = this.checkout(version)

    const keyIter = getNextKeyFromBTree(snapshot, range, numRun)
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

      if (rangeSplit[gtX].compare(rangeSplit.lte) > 0) {
        throw Error('wrong order')
      }

      tasks.push(new Promise((resolve, reject) => {
        pool.runTask({
          // TODO Pass all relevant options for threads in a transferable format
          dbOpts: this.originalOpts,
          version,
          directory: this.directory,
          range: rangeSplit
        }, (err, msg) => {
          if (!err) return resolve(msg)
          reject(err)
        })
      }))

      carry = higher
    }

    return Promise.all(tasks)
      .then((results) => results.flat())
      .finally(pool.close.bind(pool))
  }
}
