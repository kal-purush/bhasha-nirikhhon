/*
 * @lc app=leetcode id=66 lang=javascript
 *
 * [66] Plus One
 *
 * https://leetcode.com/problems/plus-one/description/
 *
 * algorithms
 * Easy (41.50%)
 * Likes:    952
 * Dislikes: 1689
 * Total Accepted:    428.6K
 * Total Submissions: 1M
 * Testcase Example:  '[1,2,3]'
 *
 * Given a non-empty array of digits representing a non-negative integer, plus
 * one to the integer.
 * 
 * The digits are stored such that the most significant digit is at the head of
 * the list, and each element in the array contain a single digit.
 * 
 * You may assume the integer does not contain any leading zero, except the
 * number 0 itself.
 * 
 * Example 1:
 * 
 * 
 * Input: [1,2,3]
 * Output: [1,2,4]
 * Explanation: The array represents the integer 123.
 * 
 * 
 * Example 2:
 * 
 * 
 * Input: [4,3,2,1]
 * Output: [4,3,2,2]
 * Explanation: The array represents the integer 4321.
 * 
 */
/**
 * @param {number[]} digits
 * @return {number[]}
 */
var plusOne = function (digits) {
    var borrow = 0
    digits[digits.length - 1] += 1
    for (var i = digits.length - 1; i >= 0; i--) {
        if (digits[i] + borrow >= 10) {
            digits[i] = digits[i] + borrow - 10
            borrow = 1
        } else {
            digits[i] += borrow
            borrow = 0
            return digits.concat()
        }
    }
    if (borrow != 0) {
        digits.unshift(borrow)
    }
    return digits.concat()
};

// console.log(plusOne([2, 9, 9, 9]))