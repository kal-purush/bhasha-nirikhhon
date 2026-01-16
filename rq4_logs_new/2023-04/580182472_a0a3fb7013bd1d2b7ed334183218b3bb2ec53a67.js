// https://leetcode.com/problems/longest-common-prefix/description/

function longestCommonPrefix(strs) {
  let smallestWord = 0
  for (const word of strs) {
    if (word.length > smallestWord) smallestWord = word.length
  }

  let str = ""
 for (let i = 0; i < smallestWord; i += 1) {
    const currentLetter = strs[0][i]
    let matchCount = 1;
    for (let j = 1; j < strs.length; j += 1) {
      const currentWord = strs[j]
      if (currentLetter === currentWord[i]) {
        matchCount += 1
      }
    }
    if (matchCount === strs.length) {
      str += currentLetter
    } else {
      break;
    }
  }

  return str
};