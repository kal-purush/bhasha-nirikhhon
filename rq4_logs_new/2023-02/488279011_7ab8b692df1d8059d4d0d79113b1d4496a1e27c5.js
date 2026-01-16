const utility = (nums, index, array, tempArray, k) => {
  if (tempArray.length === k) {
    array.push([...tempArray]);
    return;
  }

  while (index < nums.length) {
    tempArray.push(nums[index]);
    utility(nums, index + 1, array, tempArray, k);
    tempArray.pop();
    index++;
  }
};

/**
 * @param {number[]} nums
 * @return {number[][]}
 */
var subsets = function(nums) {
  let array = [];
  let tempArray = [];
  for (let i = 0; i <= nums.length; i++) {
    utility(nums, 0, array, tempArray, i);
  }

  return array;
};

console.log(subsets([1, 2, 3]));