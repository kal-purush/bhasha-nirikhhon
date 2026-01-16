const largestNumInArrays = (arr) => {
  let maxes = [];
  for(let i = 0; i <arr.length; i++){
    let tempMax = arr[i][0]
    for(let j = 0; j < arr[i].length; j++) {
      let currentElement = arr[i][j];
      if(currentElement >= tempMax) {
        tempMax = currentElement;
      }
    }
    maxes.push(tempMax);
  }
  return maxes;
}
console.log(largestNumInArrays([[4,5,1,3], [13,27,18,26],[32,25,37,39],[1000,1001,857,1],[32,25,37,39]]))
