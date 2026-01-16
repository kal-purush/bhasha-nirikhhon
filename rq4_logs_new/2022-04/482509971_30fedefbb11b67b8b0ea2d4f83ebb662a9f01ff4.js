//1. ways to print in javascript
//console.log("Hello World");
//alert("me");
//document.write("this is document write")


//2. Javascript cosole API
//console.log("Hello World", 4 + 6, "Another log");
//console.warn("this is warning");
//console.error("this is error");

//3.Javascript Veriables
// What are veriables? - Containers to stoare data values
var number1 = 34;
var number2 = 56;
console.log(number1 + number2);

//4.Data types in Javascript
//Number
var num1 = 455;
var num2 = 56.76;

//String
var str1 = "This is a String";
var str2 = 'This is a String';

//objects
var marks ={
    ravi: 34,
    shubham: 78,
    harry: 99.977
}
console.log(marks);

//Refernce data type = Array and Objects

var arr = [1,2,3,4,5];
var arr = [1,2,"String",4,5];

function avg(a,b)
{
     c = (a + b)/2;
     return c;
}

c1 = avg(4,6); 
c2 = avg(14,16); 
console.log(c1,c2);

//Conditionals in javascript

var age = 41;
//single if statement
if(age > 18){
    console.log("You can drink");
    }
else{
    console.log("You cannot drink rasna water");
}


var arr = [1,2,3,4,5,6,7];
console.log(arr);
for(var i=0<arr.length;i++)
{
    console.log(arr[i])
}