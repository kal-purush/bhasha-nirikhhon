/**
 * Add Binary Strings
 *
 * Source: https://leetcode.com/problems/add-binary
 */

const addBinaryStrings = (string1: string, string2: string): string => {
  let result = ''
  let index1 = string1.length - 1
  let index2 = string2.length - 1
  let carryNum = 0

  while (index1 >= 0 || index2 >= 0 || carryNum > 0) {
    const digit1 = Number((index1 >= 0) ? string1[index1] : 0)
    const digit2 = Number((index2 >= 0) ? string2[index2] : 0)
    let sum = digit1 + digit2 + carryNum

    if (sum > 1) {
      sum = 0
      carryNum = 1
    } else {
      carryNum = 0
    }

    result = sum.toString() + result
    index1 -= 1
    index2 -= 1
  }

  return result
}

export default addBinaryStrings