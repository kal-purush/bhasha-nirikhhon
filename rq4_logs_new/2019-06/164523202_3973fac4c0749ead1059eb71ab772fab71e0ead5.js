// Run time complexity
// 100ms fastest 80ms bottom 30% of solutions
// Memory complexity
// 41.9MB smallest 41.5 MB top 3%

// var productExceptSelf = function(nums) {
//   let productArray = [];
//   let product = 1;
//   let zeroes = 0;
//   let zeroLocation = 0;
//
//   for (let i = 0; i < nums.length; i++){
//     if (nums[i] === 0) {
//       zeroes += 1;
//       if (zeroes === 2) { break; }
//       zeroLocation = i;
//     } else {
//       product *= nums[i];
//     }
//   }
//
//   if (zeroes >= 1) {
//     for (let i = 0; i < nums.length; i++){ productArray.push(0); }
//     if (zeroes === 1) { productArray[zeroLocation] = product; }
//   } else {
//     for (let i = 0; i < nums.length; i++){ productArray.push(product / nums[i]); }
//   }
//
//   return productArray;
// };

// Desired solution
// The reason to avoid division is because dividing by zero will cause an error.
// Run time complexity
// 84ms fastest 80ms top 16% of solutions
// Memory complexity
// 42.2MB smallest 41.5 MB top 14%

var productExceptSelf = function(nums) {
  let productArray = [];
  let left = 1;
  let right = 1;

  for (let i = nums.length-1; i>=0; i--){
    productArray[i] = right;
    right *= nums[i];
  }

  for (let i = 0; i < nums.length; i++) {
    productArray[i] *= left;
    left *= nums[i];
  }

  return productArray;
};



console.log(productExceptSelf([1,2,3,4]));
console.log(productExceptSelf([0,1,2,3,4]));
console.log(productExceptSelf([0,0,1,2,3,4]));