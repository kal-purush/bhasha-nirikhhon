function missingPositiveNumber(arr) {
  let newArr = [];
  let min = Math.min(...arr);
  let max = Math.max(...arr);
  for (i = min; i < max; i++) {
    if (arr.indexOf(i) < 0 && i % 2 == 0) {
      newArr.push(i);
    }
  }
  return "prints missing positive number " + newArr;
}
let arr = [1, 2, 3, 4, 7, 10];
console.log(missingPositiveNumber(arr));