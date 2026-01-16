// console.log(count);
// 変数宣言、初期化、初期値
let count = 0;
// console.log(count);
count = 10;
// console.log(count);
let newcount = 0;

// 基本的には定数を使う
const daysInWeek = 7;

// JavaScriptの変数名はキャメルケースがメジャー
let tomatoCountNumber;
let tomato$Count;
let _tomato_Count;
let tomato2Count2;
let トマトカウント;
let tomatocount;

let additionResult = 7 + 5;
// console.log(additionResult);

let result = 5;
result = additionResult;
result += 10;
console.log(result++);
result;
// console.log(result);

// ++resultは＋１した後の値を返し、result++は＋１する前の値を返す

// 17
// number型
let number = 10;
number = -4;
number = 32.42;
// string型
let string = 'Hello';
number = 'something'; //これはできる。常にどんな方でも入れられる。動的型付け言語

// 18
const userName = 'Yoshipi';
string = 'Hello ' + userName + '!';
string = `Hello ${userName}!`;
string = "'";
string = '\'\\"'; // \はエスケープ文字
string = 'Hello Yoshipi!\ntatata';
string = `Hello Yoshipi!





tatata`; // バッククォートで改行も入れられる
console.log(string);

//20
const userInput = '10';
let calcResult;
calcResult = Number(userInput) + 10;
console.log(calcResult);
calcResult = parseInt(userInput) + 10;
console.log(calcResult);
calcResult = parseFloat(userInput) + 10;
console.log(calcResult);
calcResult = +userInput + 10;
console.log(calcResult);

const tenNumber = 10;
calcResult = '10' + String(tenNumber);
calcResult = '10' + tenNumber.toString();
console.log(calcResult);

// 21
let boolean = true;
boolean = false;

// 22
let array = ['apple', 'banana', 'orange'];
appay = [1, 2, 3, 4, 5];
array = [1, 'apple', true, null, array];
console.log(array[1]);
array = [];
array.push('apple');
console.log(array);

// 23
const coffee = {
  name: 'Chocolate Mocha', //プロパティというときは、name: 'Chocolate Mocha'の部分
  size: 350,
  isHot: true,
  toppings: ['chocolate', 'marshmallow'],
  nutritions: {
    calorie: 450,
    sugar: 40,
  },
};
coffee.isHot = false;
coffee.barista = 'Yoshipi';
console.log(coffee.barista);

// 24
//何もない値を表すものは、javascriptでは2つある。nullとundefined
// undefinedは何もないを表す。エラーのようなもの。基本的に = undefinedは使わない。
// nullは何もないことを明示的に示す。
let userInfo = null;
let userInfo2;
// userinfo = undefined;

// 25
// typeof演算子
// typeof 10; // number
// typeof [1,2,3]; // object
// typeof null; // object
// typeof undefined; // undefined

// 26
// 関数（何度も呼び出すことができるコードの塊）
function tmp(num1, num2) {
  console.log(1 + 1);
} // オブジェクト以外の波括弧を「ブロック」という

// 27
// add(仮引数1, 仮引数2) 仮引数はparameterともいう
function add(num1, num2) {
  console.log(num1 + num2);
}
// add(引数1, 引数2)　引数はargumentともいう
add(2, 3);
add(6, 3);
add(10, 5);

// 28
function add2(num1, num2) {
  return num1 + num2;
  console.log('hello'); // returnの後は無視される
}
const returnedValue = add2(2, 3);
console.log(returnedValue);

//29 関数の位置
// 関数宣言は巻き上げられる
const result2 = add3(2, 3);
console.log(result2);
function add3(num1, num2) {
  return num1 + num2;
}

// 30 スコープ
// スコープ：変数を参照できる範囲。valueはadd4の中がスコープ
// スコープが全ての場所である：グローバルスコープ
// グローバルスコープの変数：グローバル変数
// 特定の場所のみで参照できる変数：ローカル変数
// ローカル変数のスコープ：ローカルスコープ
// 関数のスコープを関数スコープという
const value = 'hello';

function add4(num1, num2) {
  const value = num1 + num2;
  console.log(value); // 外側にあるものを内側が隠すことを、シャドーイングという。
  return value;
}

const returnedValue2 = add4(6, 3);
// console.log(value); //エラー

// 31
// コメント
// 自由にコメントできる。末にも書ける。
/* ここにもいろいろ書ける。
   複数行書ける */
const /* これもできる */ value2 = 'hello';