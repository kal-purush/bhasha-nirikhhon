console.clear();

//prompts the user for number inputs
let number1 = prompt("Enter the number1");
let number2 = prompt("Enter the number2");
let number3 = prompt("Enter the number3");

// gives the largest number
function getLargestNumber(num1,num2,num3){
    return Math.max(num1,num2,num3);
}

//invoking getLargestNumber()
let largestNumber = getLargestNumber(number1,number2,number3);
console.log(largestNumber);
alert(largestNumber);