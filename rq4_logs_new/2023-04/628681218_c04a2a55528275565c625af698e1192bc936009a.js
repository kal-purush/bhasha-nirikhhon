//Given an array of numbers and a target number, find a pair of numbers in the array that sums to the target number.

var twoSum = (nums, target) => {
  //inputs: nums<number[]> target<number>
  let x = {}; // {number : index} example: {3:0, 15:1, 5:2}
  let i = 0; //index
  for (num of nums) {
    if (x[num] !== undefined)
      //if x[num] would return the index of a number already added, if it exists
      return [x[num], i]; //return indexes of past and current number
    x[target - num] = i; // !!Object x holds the difference to the target number, not the original number
    i++; //increment index
  }
  return [];
};