// sum all numbers till the given one.


function sum(n) {
	return n ? n + sum(n-1) : 0;
}

//console.log(sum(5)); // 15

console.log(sum(100)); // 5050