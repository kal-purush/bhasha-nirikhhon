const solution = function(firstArray, secondArray) {
  let arr = [];
 firstArray.forEach((x, index) => {
    let diff = Math.abs(x - secondArray [index]);
    arr.push(diff*diff);
 })
  return (arr.reduce((acc, curr) => acc + curr))/arr.length;
}