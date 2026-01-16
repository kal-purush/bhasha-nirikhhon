/*
  자료구조(Data Structure)
    - 순서(index)를 가지고 있음. 다양한 타입이 공존 -> 보완하기 위해 타입 배열이 있음
    - 연속적으로 이어져 있지 않음 (일반적으로는 연속으로 이어져 있음)
    - 오브젝트와 거의 동일
*/

// 배열 생성 방법
let array1 = new Array(3); // Array 객체(클래스). 3개짜리 배열을 만듦
console.log(array1);

let array2 = new Array(1, 2, 3); // Array 객체(클래스)이용. 실제 아이템 전달
console.log(array2);

let array3 = Array(1, 2, 3, 4); // 스태틱 함수 이용
console.log(array3);

let array4 = [1, 2, 3, 4, 5]; // 배열 리터럴(구체적인 값을 가지고 배열을 생성)
console.log(array4);
// 오브젝트에서는 {}, 배열에서는 []

let array5 = Array.from(array4); // 기존 배열로부터 새로운 배열을 만듦
console.log("array5", array5);

let array6 = Array.from("Monday");
console.log("array6", array6);
// 문자(char) 하나하나를 배열의 아이템으로
// iterable (순회, 반복이 가능)

let array7 = Array.from({
  0: "안",
  1: "녕",
  2: "하",
  3: "세",
  4: "요",
  length: 5 // 필수. 배열에는 길이 정보가 있음
});
console.log(array7);