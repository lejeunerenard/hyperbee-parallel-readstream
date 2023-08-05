# Hyperbee Parallel ReadStream

**Note: Currently experimental, not production ready**

This module wraps the [`Hyperbee`](https://github.com/holepunchto/hyperbee)
class to add the `parallelReadStream()` method. `parallelReadStream()` will
create worker threads to spread the task across multiple cores allowing for
faster reading of nodes on larger databases & ranges.

## Usage

```js
import HyperbeeParallel from '@lejeunerenard/hyperbee-parallel-readstream'

const db = new HyperbeeParallel(core, {
  keyEncoding: 'utf-8',
  valueEncoding: 'json'
})
const range = { gte: 'beep', lt: 'boop' }
for await (const node of db.parallelReadStream(range)) {
  console.log(node)
}
```

See [`example.js`](example.js) for an example on a large data set (1 million
keys).

## TODO

- [ ] Add support for `createReadStream()`'s second argument `opt`
- [ ] Add support for limit being distributed on workers
- [ ] Further test how keys are split based on `hyperbee`'s b-tree
