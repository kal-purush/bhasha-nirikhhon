const data = [1, 2, 3, 'David', true];
// 값 변경
data[1] = 77;
// 값들 도려내기
data.splice(1, 3);
console.log(data);
// [1, 2, 3, 'David', true] - [2, 3, 'David'] => [1, true]
console.log(data.length);

/* 객체로 배열 생성하기 
const data = new Array();
data[1] = 1;
data[2] = 2;
console.log(data);
console.log(data[0]);
*/

// Object in Array example
const data2 = [
    {name: 'David', age: 30 }
]