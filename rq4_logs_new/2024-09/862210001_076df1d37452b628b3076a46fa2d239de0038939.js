// let score = "33abc"
let score = true
console.log(typeof score);
// OR console.log(typeof(score));//

let valueInNumber = Number(score)
console.log(typeof valueInNumber  );
console.log(valueInNumber);

// "33"=> 33
//"33abc"=> NaN
// true=>1 / false=>0
//undefined=>NaN
// null=> 0

let isLoggedIn = ""
let booleanIsLoggedIn = Boolean(isLoggedIn)
console.log( booleanIsLoggedIn);

// 1=> true 0=> false
// "" => false
//"ajit" => true

let someNumber =33
let stringNumber = String(someNumber)
console.log(stringNumber);
console.log(typeof stringNumber);
