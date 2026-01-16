console.log("test");

let example = [1, 2, 3, 4, 5, 6, 7, 8, 9];

let customIndexOf = (arr, value) => {
  for (const elementIndex in arr) {
    if (arr[elementIndex] === value) {
      return elementIndex;
    } else {
      return -1;
    }
  }
};
console.log(customIndexOf(example, 1));