// Write a function joinArraysById in JavaScript that takes two arrays, arr1 and arr2, as input.
//  Each array contains objects that have an id field with an integer value.
// The function should merge arr1 and arr2 based on their id key and return a new array,
// joinedArray, as the result.

function joinArraysById(arr1, arr2) {
  let joinedObject = {};
  arr1.concat(arr2).forEach((e) => {
    if (!joinedObject[e.id]) {
      joinedObject = { ...joinedObject, [e.id]: e };
    } else {
      joinedObject = {
        ...joinedObject,
        [e.id]: { ...joinedObject[e.id], ...e },
      };
    }
  });
  let joinedArray = Object.values(joinedObject);
  return joinedArray;
}

const arr1 = [
  { id: 1, x: 1 },
  { id: 2, x: 9 },
];
const arr2 = [{ id: 3, x: 5 }];

console.log(joinArraysById(arr1, arr2));
// // Output: [
// // { "id": 1, "x": 1 },
// // { "id": 2, "x": 9 },
// // { "id": 3, "x": 5 }
// // ]

// const arr1 = [
// { id: 1, x: 2, y: 3 },
// { id: 2, x: 3, y: 6 },
// ]
// const arr2 = [
// { id: 2, x: 10, y: 20 },
// { id: 3, x: 0, y: 0 },
// ]

// console.log(joinArraysById(arr1, arr2))
// // Output: [
// // { "id": 1, "x": 2, "y": 3 },
// // { "id": 2, "x": 10, "y": 20 },
// // { "id": 3, "x": 0, "y": 0 }
// // ]

// const arr1 = [{ id: 1, b: { b: 94 }, v: [4, 3], y: 48 }]
// const arr2 = [{ id: 1, b: { c: 84 }, v: [1, 3] }]

// console.log(joinArraysById(arr1, arr2))
// // Output: [
// // { "id": 1, "b": { "c": 84 }, "v": [1, 3], "y": 48 }
// // ]