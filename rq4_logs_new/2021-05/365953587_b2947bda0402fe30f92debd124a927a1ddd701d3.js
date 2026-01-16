var hello = "안녕하세요";
document.body.innerText = hello;

var two = "이";
var value = 1 / "A"; // NaN
var value2 = 0 / 0; // NaN
var value3 = 123131 / 0; // infinity
var value4 = 0 / 112331; // 0
var value5 = null; // null의 typeof 는 object

console.log(value);
console.log(value2);
console.log(value3);
console.log(value4);
console.log(value5);

// % 연산자 (나머지)
console.log(5 % 2.3); // IEEE 유동소수점 처리 때문에 0.40000000000000036
console.log((5 * 10 - ( 2 * 2.3 * 10 )) / 10); // 0.4
// A(피제수) / B(제수)로 나누면 C(몫) 과 R(나머지)가 나옴
// A = BC + R (피제수 = (몫x제수) + 나머지)
// R = A - BC
// 피제수 - (몫 x 제수) = 나머지
console.log("A" > 1);


var j = 1;
var hol = 0;
var jak = 0;

for(var i=1; i<=4; i++){
    if(i % 2 != 0){
        hol += i;
    } else {
        jak += i;
    }
}
console.log("홀수 " + hol);
console.log("짝수 " + jak);