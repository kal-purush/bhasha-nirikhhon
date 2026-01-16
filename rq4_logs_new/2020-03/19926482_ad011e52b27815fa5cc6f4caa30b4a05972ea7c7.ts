/**
 * Roman numeral to integer
 *
 * Source: https://leetcode.com/problems/roman-to-integer/
 */

const CHARS_TO_NUMS: { [prop: string]: number } = {
  I: 1,
  V: 5,
  X: 10,
  L: 50,
  C: 100,
  D: 500,
  M: 1000,
}

const romanToInteger = (romanString: string): number => {
  let i = 0
  let sum = 0

  while (i < romanString.length) {
    const currentChar = romanString[i]
    const nextChar = (i < (romanString.length - 1)) ? romanString[i + 1] : ''
    const currentNumber = CHARS_TO_NUMS[currentChar]
    const nextNumber = CHARS_TO_NUMS[nextChar]
    const nextMagnitude = currentNumber * 10

    // Or, if currentNumber < nextNumber, do the subtraction
    if (nextNumber && (nextNumber === nextMagnitude || nextNumber === (nextMagnitude / 2))) {
      sum += (nextNumber - currentNumber)
      i += 2
    } else {
      sum += currentNumber
      i += 1
    }
  }

  return sum
}

export default romanToInteger