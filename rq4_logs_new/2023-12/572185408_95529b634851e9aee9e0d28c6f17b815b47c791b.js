// A list of snippets which may generally be useful

/**
 * Given input as lines, creates an array of linked nodes (up, dn, lt, rt)
 */
const nodes = input.map((line, row) => [...line].map((char, col) => ({ id: `c${col}r${row}`, char, col, row }))).flat()
const [height, width] = [input, input[0]].map(arr => arr.length)
nodes.forEach((node, i) => {
  const { row, col } = node
  const [up, dn, lt, rt] = [[row-1,col], [row+1,col], [row,col-1], [row,col+1]].map(pos => nodes.find(el => el.row === pos[0] && el.col === pos[1]))
  Object.assign(node, { up, dn, lt, rt })
})
return { nodes, height, width }


// General memoizing function, with .cache property for inspection
function memoize(fn) {
  const cache = {}
  function memo(...args) {
    const key = JSON.stringify(args) // more adaptable
    // const key = args.join(' ') // better for this case
    if (cache[key] !== undefined) return cache[key]
    const result = fn(...args)
    cache[key] = result
    return result
  }
  memo.cache = cache
  return memo
}