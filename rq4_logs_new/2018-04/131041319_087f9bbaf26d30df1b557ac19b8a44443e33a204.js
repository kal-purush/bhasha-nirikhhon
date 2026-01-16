let addVar = 1;
let subtractVar = 2;
let multiplyVar = 3;
let divideVar = 4;

function add(a,b){
	console.log(a+b);
}
function divide(a,b){
	console.log(a/b);
}
function multiply(a,b){
	console.log(a*b);
}
function subtract(a,b){
	console.log(a-b);
}
function operate(op,num1,num2){
	if(op === 1){
		console.log(add(num1,num2))
	}else if(op === 2){
		console.log(subtract(num1,num2))
	}else if(op === 3){
		console.log(multiply(num1,num2))
	}else if(op === 4){
		console.log(divide(num1,num2))
	}
}
operate(4,200,2)