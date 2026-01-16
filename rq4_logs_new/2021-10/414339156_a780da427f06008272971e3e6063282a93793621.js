const allConstruct = (target, wordBank, memo = {}) => {
  if (target in memo) {
    return memo[target]
  }
  if (target === "") {
    return [[]]
  }
  let ways = []
  for (let word of wordBank) {
    if (target.indexOf(word) === 0) {
      let suffix = target.slice(word.length)
      const result = allConstruct(suffix, wordBank, memo)
      const way = result.map(result => [word, ...result])
      ways.push(...way)
    }
  }
  memo[target] = ways
  return memo[target]
}

console.log(allConstruct("hello", ["cat", "dog", "mouse"])) // []
console.log(allConstruct("purple", ["purp", "p", "ur", "le", "purpl"])) // [["purp", "le"], ["p","ur","p","le"]]
console.log(allConstruct("abcdef", ["ab", "abc", "cd", "def", "abcd", "ef", "c"]))
/**
[
  [ 'ab', 'cd', 'ef' ],
  [ 'ab', 'c', 'def' ],
  [ 'abc', 'def' ],
  [ 'abcd', 'ef' ]
]
*/
console.log(allConstruct("skateboard", ["bo", "rd", "ate", "t", "ska", "sk", "boar"])) // []
console.log(allConstruct("eeeeeeeeeeeeeeeeeeeeeeeeeeef", ["e", "ee", "eee", "eeee", "eeeee"])) // []
console.log(allConstruct("", ["x", "y", "z"])) // [[]]