var count, fizzer, buzzer, fizzbuzzer;
count = function(start, end) {
	// initialize the output variable
	var output = buzzer(fizzer(fizzbuzzer(start)));

	// modify output to contain the desired sequence
	for (var i = start; i < end; i += 1) {
		// stuff here gets repeated
		output = output + " " + buzzer(fizzer(fizzbuzzer(i + 1)));
	}

	// return the output, e.g. "1 2 fizz 4 buzz"
	return output;
};

fizzer = function(num) {
	// if (0 === num % 3) {
	// 	return "fizz";
	// }
	// return num;
	return 0 === num % 3 ? "fizz" : num;
};

buzzer = function(num) {
	return 0 === num % 5 ? "buzz" : num;
};

fizzbuzzer = function(num) {
	// return (0 === num % 3) && (0 === num % 5) ? "fizzbuzz" : num;
	// return 0 === (num % 3 + num % 5) ? "fizzbuzz" : num;
	return 0 === num % 15 ? "fizzbuzz" : num;
};
module.exports = {
	count: count,
	fizzer: fizzer,
	buzzer: buzzer,
	fizzbuzzer: fizzbuzzer
};