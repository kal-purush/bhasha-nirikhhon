//The function should return an array containing repetitions of the number argument. For instance, replicate(3, 5) should return [5,5,5]. If the times argument is negative, return an empty array.
const replicate = (repNum, numTo) => {
  let arr = [];

  if (repNum <= 0) return [];
  arr.push(numTo);
  return arr.concat(replicate(repNum - 1, numTo));
};
console.log(replicate(3, 5)); // [5, 5, 5]
console.log(replicate(1, 69)) // [69]
console.log(replicate(-2, 6)) // []