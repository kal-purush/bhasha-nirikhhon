/*var a: string = "hello world";
console.log(a);
*/
/*
var a: number = 6.2222222;


var b: boolean = true;
var c: string = "hello";
var d: number[] = [1, 2, 3];
console.log(a,b,c,d);
*/

const a = 5;
const b : number = 33;
const c ="best";

//I suggest use let instead of var everywhere, becuase let has blocked scope
if (true) {
	let a = 4;
console.log("let: " + a);// Error: a is not defined in this scope	
	//use a
}
else {
	let a = "string";
	//use a
}
console.log("let: " + a);// Error: a is not defined in this scope