import b4a from 'b4a'

export function lexicographicMidPoint (low, high) {
  const bytes = []
  let carry = 0
  for (let i = 0; i < high.length; i++) {
    const byteLow = i >= low.length ? 0 : low[i]
    const byteHigh = high[i]
    const diff = (byteHigh - byteLow)
    const mid = diff >> 1
    bytes.push(mid + carry * 128 + byteLow)
    carry = diff - 2 * mid
  }
  // Hanging Carry check
  if (carry !== 0) {
    bytes.push(carry * 128)
  }
  return b4a.from(bytes)
}

export function bytewiseSubtract (low, high) {
  const bytes = []
  for (let i = 0; i < high.length; i++) {
    const byteLow = i >= low.length ? 0 : low[i]
    const byteHigh = high[i]
    const diff = (byteHigh - byteLow)
    bytes.push(diff)
  }
  return b4a.from(bytes)
}

export function bytewiseAddition (a, b) {
  const bytes = []
  let carry = 0
  let longer
  let shorter
  if (a.byteLength >= b.byteLength) {
    longer = a
    shorter = b
  } else {
    longer = b
    shorter = a
  }

  let foundValue = false
  for (let i = longer.length - 1; i >= 0; i--) {
    const byteLow = i >= shorter.length ? 0 : shorter[i]
    const byteHigh = longer[i]
    const add = (byteHigh + byteLow)
    const remainder = add % 256
    const val = remainder + carry
    carry = Math.floor(add / 256)
    if (!foundValue && val === 0) continue
    foundValue = true
    bytes.unshift(val)
  }
  // Hanging Carry check
  if (carry !== 0) {
    bytes.unshift(carry * 256)
  }
  return b4a.from(bytes)
}

// export function bytewiseMultiple (buf, factor) {
//   const bytes = []
//   let carry = 0
//   for (let i = buf.length - 1; i >= 0; i--) {
//     const byte = buf[i]
//     const division = byte * factor
//     const nonRemainder = Math.floor(division)
//     const remainder = division - nonRemainder
//     bytes.unshift(nonRemainder + carry * 256)
//     carry = remainder
//   }
//   // Hanging Carry check
//   if (carry !== 0) {
//     bytes.unshift(carry * 256)
//   }
//   return b4a.from(bytes)
// }

export function bytewiseDivide (buf, denominator) {
  const bytes = []
  let carry = 0
  for (let i = 0; i < buf.length; i++) {
    const byte = buf[i]
    const division = byte / denominator
    const nonRemainder = Math.floor(division)
    const remainder = division - nonRemainder
    bytes.push(nonRemainder + carry * 256)
    carry = remainder
  }
  // Hanging Carry check
  if (carry !== 0) {
    bytes.push(carry * 256)
  }
  return b4a.from(bytes)
}
