// binary_search.js

// 通过二分查找法实现对数组的搜索函数，返回找到元素的下标
// 没有找到元素则返回 -1
/**
 * @param {number[]} arr 待搜索的数组
 * @param {number} num 待查找的数字
 * @returns number
 */
function findIndex(arr, num) {
  // todo 代码实现
  let left = 0
  let right = arr.length

  while (left < right) {
    let mid = left + Math.floor((right - left) / 2)
    if (arr[mid] > num) {
      right = mid - 1
    } else if (arr[mid] < num) {
      left = mid + 1
    } else if (arr[mid] === num) {
      return mid
    }
  }

  return -1;
}
let a = [1, 2, 3, 7, 12, 25, 33, 54];
console.log(findIndex(a, 3)); // 2
console.log(findIndex(a, 33)); // 6
console.log(findIndex(a, 28)); // -1

// export default findIndex;