const twoSum = function (numbers, target) {
  let left = 0; // Initialize the left pointer to the start of the array
  let right = numbers.length - 1; // Initialize the right pointer to the end of the array

  while (left < right) {
    // Loop while the left pointer is less than the right pointer
    const sum = numbers[left] + numbers[right]; // Calculate the sum of the numbers at the left and right pointers

    if (sum === target) {
      // If the sum is equal to the target
      return [left + 1, right + 1]; // Return an array with the indices (1-indexed) of the two numbers
    } else if (sum < target) {
      // If the sum is less than the target
      left++; // Move the left pointer to the right (increment it by 1)
    } else {
      // If the sum is greater than the target
      right--; // Move the right pointer to the left (decrement it by 1)
    }
  }
  return []; // No solution found, return an empty array
};

module.exports = twoSum;