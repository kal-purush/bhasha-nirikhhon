// Given an array nums, write a function to move all 0's to the end of it while maintaining the relative order of the non-zero elements.

// For example, given nums = [0, 1, 0, 3, 12], after calling your function, nums should be [1, 3, 12, 0, 0].

// Note:
// You must do this in-place without making a copy of the array.
// Minimize the total number of operations.

// iterate through array // if find a zero splice it out and increment count
// at the end push on # of zeros === count
'use strict'
const moveZeroes = (nums) => {
  // splicing will change our array so by starting at the end we won't miss anything
  for (let i = nums.length-1; i>=0; i--){
    if (nums[i] === 0){
      nums.splice(i, 1);
      nums.push(0);
    }
  }

  console.log(nums);
  return nums; 
};

moveZeroes([0,0,1]);
moveZeroes([0, 1, 0, 3, 12]);