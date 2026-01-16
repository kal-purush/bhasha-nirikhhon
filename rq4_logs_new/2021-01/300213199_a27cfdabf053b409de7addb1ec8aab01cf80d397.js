// 168. Excel表列名称
// https://leetcode-cn.com/problems/excel-sheet-column-title/
// 给定一个正整数，返回它在 Excel 表中相对应的列名称。
// 输入: 1
// 输出: "A"
// 输入: 28
// 输出: "AB"

/**
 * @param {number} n
 * @return {string}
 */
// 思路：
// 1. 首先要让输入 26 时能返回 'z', 'z' 在字符串第 25 位，所以 n = n-1
// 2. 使用求余数获取剩余的值 less
// 3. 使用除法判断是否存在大于 26 的倍数 times，如果存在，说明前面有值，记作 beforeLetter
// 4. 如果 times 仍然大于 26，递归调用，获取进一步的 beforeLetter，否则直接使用 times-1

let letters = 'abcdefghijklmnopqrstuvwxyz'
var convertToTitle = function(n) {
    n = n - 1
    let times = Math.floor(n / 26)
    let less = n % 26
    let ans = letters[less]
    if (times > 0) {
        let beforeLetter = times >= 26 ? convertToTitle(times) : letters[times-1]
        ans = beforeLetter + ans
    }
    return ans.toUpperCase()
};