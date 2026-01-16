// Yon need to return an array containing the keys of an object
// whose value's square root is an integer.

// SOLUTION 1

function squareRootIntegerKeys(obj) {
  let keys = Object.keys(obj);
  let final = [];
  for (let letter in obj) {
    obj[letter] = Math.sqrt(obj[letter]);
  }
  let arr = Object.values(obj).filter((v) => {
    if (Number.isInteger(v)) return v;
  });
  for (let i = 0; i < arr.length; i++) {
    keys.forEach((k) => {
      if (obj[k] === arr[i]) return final.push(k);
    });
  }
  return final;
}

// SOLUTION 2

function squareRootIntegerKeys(obj) {
  const entries = Object.entries(obj);
  const result = [];
  entries.forEach((el) => {
    const square = Math.sqrt(el[1]);
    if (Number.isInteger(square)) {
      result.push(el[0]);
    }
  });
  return result;
}

console.log(squareRootIntegerKeys({ a: 9, b: 52, c: 4 })); // ['a', 'c']
console.log(squareRootIntegerKeys({ a: 18, b: 81, c: 65, d: 144, e: 100 })); // ['b', 'd', 'e']