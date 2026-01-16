const snail = (array) => {
  let finalArray = [];
  while (array.length) {
    finalArray.push(...array.shift());
    for (var i = 0; i < array.length; i++) {
      finalArray.push(array[i].pop());
    }
    finalArray.push(...(array.pop() || []).reverse());
    for (var i = array.length - 1; i >= 0; i--) {
      finalArray.push(array[i].shift());
    }
  }
  return finalArray;
};
let arrLittle = [
  [1, 2, 3],
  [4, 5, 6],
  [7, 8, 9],
];
let arrBig = [
  [1, 2, 3, 4, 5],
  [6, 7, 8, 9, 10],
  [11, 12, 13, 14, 15],
  [16, 17, 18, 19, 20],
  [21, 22, 23, 24, 25],
];

console.log(snail(arrLittle));