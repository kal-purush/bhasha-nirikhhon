const assertEqual = function(actual, expected) {
  if (actual !== expected) {
    return `❌ Assertion Failed: ${actual} !== ${expected}`;
  }
  
  return `✅ Assertion Passed ${actual} === ${expected}`;
};

const eqArrays = function(arr1, arr2) {
  if (arr1.length !== arr2.length) {
    return false;
  }
  
  for (let i = 0; i < arr1.length; i++) {
    if (arr1[i] !== arr2[i]) {
      return false;
    }
  }

  return true;
};

const eqObjects = function(obj1, obj2) {
  const firstObjSize = Object.keys(obj1).length;
  const secondObjSize = Object.keys(obj2).length;

  if (firstObjSize !== secondObjSize) {
    return false;
  }

  let isTrue = false

  for (const key in obj1) {
    if (Array.isArray(obj1[key]) && Array.isArray(obj2[key])) {
      isTrue = eqArrays(obj1[key], obj2[key])
    } else if (obj1[key] !== obj2[key]) {
      return false;
    }
    
  }

  return isTrue;

}

// const ab = { a: "1", b: "2" };
// const ba = { b: "2", b: "1" };
// console.log(eqObjects(ab, ba)); // => true

// const abc = { a: "1", b: "2", c: "3"};
// console.log(eqObjects(ab, abc)); // => false

// const cd = { a: "7", b: "5" };
// const dc = { b: "5", a: "7" };
// console.log(eqObjects(cd, dc)); // => true

// const cde = { a: "4", b: "8", c: "3"};
// console.log(eqObjects(cde, cd)); // => false

const cd = { c: "1", d: ["2", 3] };
// const dc = { d: ["2", 3], c: "1" };
// console.log(eqArrays(eqObjects(cd, dc), true)); // => true

const cd2 = { c: "1", d: ["2", 3] };
console.log(assertEqual(eqObjects(cd, cd2), true)); // => false
// console.log(eqObjects(cd, cd2))