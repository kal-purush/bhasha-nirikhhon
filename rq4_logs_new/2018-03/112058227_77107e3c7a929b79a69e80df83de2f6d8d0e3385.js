export default function(levels, n, s) {
  // check per level if status per user is 1

  if (typeof n === 'number' && typeof s === 'number') {
    return levels[n].every(user => Number(user.status) == s)
  }

  let myLevel = 0
  levels.every((level, i) => {
    let result = level.every(user => Number(user.status) == 1)
    myLevel = i
    return result
  })
  return myLevel
}