//DS Interview Questions -Beginner Level
const mostFreqQuesn = (arr) => {
  let obj = {};
  const len = arr.length;
  let freq_int = -Infinity;
  let num = 0;
  for (let i = 0; i < len; i++) {
    if (obj[arr[i]]) {
      obj[arr[i]] = obj[arr[i]] + 1;
      if (freq_int < obj[arr[i]]) {
        freq_int = obj[arr[i]];
        num = arr[i];
      }
    } else {
      obj[arr[i]] = 1;
    }
  }
  return num;
};
console.log(
  "most frequent integer in an array",
  mostFreqQuesn([2, 2, 2, 4, 6, 7, 9, 9, 9, 9])
);