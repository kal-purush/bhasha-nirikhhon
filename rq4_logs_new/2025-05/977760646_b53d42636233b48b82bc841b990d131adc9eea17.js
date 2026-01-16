//Find the Smallest and Biggest Numbers

/*Create a function that takes an array of numbers 
and return both the minimum and maximum numbers, in that order.*/
function minMax(arr){
	return [Math.min(...arr), Math.max(...arr)];
}


//sum of cubes

/*Create a function that takes in an array of numbers and returns the sum of its cubes.*/
function sumOfCubes(nums) {
	return nums.map(n => n**3).reduce((a, b) => a+b, 0);
}


//Filter out Strings from an Array

/*Create a function that takes an array of non-negative integers 
and strings and return a new array without the strings.*/
function filterArray(arr) {
	return arr.filter(item => typeof item === 'number');
}


//Is the Number Symmetrical?

/*Create a function that takes a number as an argument and returns true or false depending
on whether the number is symmetrical or not. A number is symmetrical when it is the same as its reverse.*/
function isSymmetrical(num) {
	let str = num.toString();
	let reversed = str.split('').reverse().join('');
	return str === reversed;
}


//Special Arrays

/*An array is special if every even index contains an even number and every odd index contains an odd number.
Create a function that returns true if an array is special, and false otherwise.*/
function isSpecialArray(arr) {
	for (let i=0; i<arr.length; i++){
		if (i % 2 ===0 && arr[i] % 2 !== 0){
			return false;
		}
		if (i % 2 !== 0 && arr[i] % 2===0){
			return false;
		}
	}
	return true;
}


//How Much is True?

//Create a function which returns the number of true values there are in an array.
function countTrue(arr) {
	let count = 0;
	for (let i=0; i<arr.length; i++){
		if (arr[i] === true){
			count++;
		}
	}
	return count;
}


//Convert Key, Values in an Object to Array

//Write a function that converts an object into an array of keys and values.
function objectToArray(obj) {
	return Object.entries(obj);
}


//Array of Multiples

//Create a function that takes two numbers as arguments (num, length) and returns an array of multiples of num until the array length reaches length.
function arrayOfMultiples (num, length) {
	let result = [];
	for (let i = 1; i <= length; i++){
		result.push(num * i);
	}
	return result;
}


//25-Mile Marathon

//Mary wants to run a 25-mile marathon. When she attempts to sign up for the marathon,
/*she notices the sign-up sheet doesn't directly state the marathon's length. Instead,
the marathon's length is listed in small, different portions. Help Mary find out how
long the marathon actually is.
Return true if the marathon is 25 miles long, otherwise, return false.*/
function marathonDistance(d) {
	let total = d.reduce((sum, num) => sum + Math.abs(num), 0);
	return total === 25;
}


//Square Every Digit
/*Create a function that squares every digit of a number.*/
function squareDigits(n) {
	return Number(
	n
		.toString()
		.split('')
		.map(digit => Math.pow(Number(digit), 2)).join('')
	);
}