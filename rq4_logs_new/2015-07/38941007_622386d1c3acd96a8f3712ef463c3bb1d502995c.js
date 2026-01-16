"use strict";

// General Assembly
// Web Development Immersive Fundamentals
// http://fundamentals.generalassemb.ly/03_chapter/intro.html

// 3. Writing Expression in JavaScript
	// 3.1. Expression and Evaluations
		// 3.1.1. Data (a.k.a Values)
		// Use the typeof operator to return a string indicating the type of the unevaluated operand.

			typeof 'foo';
			// => 'string'

			typeof 24;
			// => 'number'

			typeof 3.45;
			// => 'number'

			typeof true;
			// => 'boolean'

			typeof false;
			// => 'boolean'

			typeof '99';
			// => 'string'
			// Is a string due to being in a single-quote or double-quote.

			typeof 'true';
			// => 'string'
			// Is a string due to being in a single-quote or double-quote.

			typeof Infinity;
			// => 'number'

			typeof NaN;
			// => 'number'

		// 3.1.2. Operators
		// Use the equality operators to compare the values.

			5 === 5;
			// => true
			// Is true because both sides (values) are completely identical.

			5 === 5.0;
			// => true
			// Is true because both sides (values) are completely identical.

			5 === '5';
			// => false
			// Is false because both sides (values) are not identical. The number 5 is not the same as the '5' string.

			5 === '5.0';
			// => false
			// Is false because both sides (values) are not identical. The number 5 is not the same as the '5.0' string.

			5 == 5;
			5 == 5.0;
			// => true
			// true == true;
			// Is true because both sides (values) are similar and 'truthy'.

			5 == '5';
			5 == '5.0';
			// => true
			// true == true;
			// Is true because both sides (values) are similar and 'truthy'.

			true == 1;
			// => true
			// Is true becuase 'true' is converted to 1 and then compared. Both sides (values) are 'truthy'.

			false == 0;
			// => true
			// Is true because 'false' is converted to 0 and then compared. Both sides (values) are 'falsey'.

			'' == '0';
			// => false
			// false == true;

			0 == '';
			// => true
			// false = false;
			// Is true because both sides (values) are 'falsey'.

			0 == '0';
			// => true

			false == '0';
			// => true

			false == 'false';
			// => false
			// false == true;

			false == undefined;
			// => false

			false == null;
			// => false

			null == undefined;
			// => true

		// Use the logical AND (&&) operators to compare the values.

			true && true;
			// => true
			// Is true because both values are 'truthy'. It will return the last 'truthy' value.

			true && false;
			// => false
			// Is false because both values are 'truthy' and 'falsey'. It will return the first 'falsey' value.

			false && true;
			// => false
			// Is false because both values are 'falsey' and 'truthy'. It will return the first 'falsey' value.

			false && (3 == 4)  // f && f returns false
			// => false
			// false && false;
			// Is false because '(3 == 4)'' is not equal to each other. It will return the first 'falsey' value.

			'foo1' && 'foo2';
			// => foo2
			// true && true;
			// Is foo2 beacuse both values are 'truthy'. It will return the last 'truthy' value.
			
			false && 'foo';
			// => false
			// false && true;
			// Is false because both values are 'falsey' and 'truthy'. It will return the first 'falsey' value
			
			'foo' && false;
			// => false
			// true && false;
			// Is false because both values are 'truthy' and 'falsey'. It will return the first 'falsey' value.

		// Use the logical OR (||) operators to compare the values.

			true || true;
			// => true
			// Is true because both values are not 'falsey'. It will return the first 'truthy' value.

			false || true;
			// => true
			// Is true because both values are not 'falsey'. It will return the first 'truthy' value.

			true || false;
			// => true
			// Is true because both values are not 'falsey'. It will return the first 'truthy' value.

			false || (3 == 4);
			// => false
			// false || false;
			// Is false because '(3 == 4)'' is not equal to each other; which creates 'false && false'. It will return the last 'falsey' value.

			'foo1' || 'foo2';
			// => foo1
			// true || true;
			// Is foo1 because both values are not 'falsey'. It will return the first 'truthy' value.

			false || 'foo';
			// => foo
			// false || true;
			// Is foo because both values are not 'falsey'. It will return the first 'truthy' value.

			'foo' || false;
			// => foo
			// true || false;
			// Is foo because both values are not 'falsey'. It will return the first 'truthy' value.

		// Use the logical NOT (!) operators to compare the values.

			!true;
			// => false
			// Is false because (!) reverses the value of the boolean.

			!false;
			// => true
			// Is true because (!) reverses the value of the boolean.

			!'foo';
			// => false
			// !'true';
			// Is false because (!) reverses the value of the boolean.

		// Use the arithemtic operators (+, -, /, *, %, ++, --).

			true + 1;
			// => 2
			// boolean + number; -> addition

			false + false;
			// => 0
			// boolean + boolean; -> addition

			5 + 'foo';
			// => 5foo
			// number + string; -> concatenation

			'foo' + false;
			// => foofalse
			// string + boolean; -> concatenation
			// Is foofalse because JavaScript converts the boolean (false) into a string ('false').

			false + 'foo';
			// => falsefoo
			// boolean + string; -> concatenation
			// Is falsefoo because JavaScript converts the boolean (false) into a string ('false').

			'foo' + 'bar';
			// => foobar
			// string + string; -> concatenation

			Infinity + Infinity;
			// => Infinity
			// number + number; -> addition
			// Is Infinity because it is beyond infinity.

			5 - 3;
			// => 2
			// number - number; -> subtraction

			3 - 5;
			// => -2
			// number - number; -> subtraction

			'foo' - 3;
			// => NaN
			// string - number;
			// Is NaN because a literal string is not a number.

			Infinity - Infinity;
			// => NaN
			// number - number; -> subtraction
			// Is NaN because you can not subtract positive and negative infinity against each other.

			1 / 2;
			// 0.5
			// number / number; -> division

			1 / -2;
			// -0.5
			// number / number; -> division
			// Is -0.5 because anything divided by a negative (-) automatically gives you a '-' numerical.

			1.0 / 2.0;
			// 0.5
			// number / number; -> division

			2.0 / 0;
			// Infinity
			// number / number; -> division
			// Is Infinity because anything divided by 0 automatically gives you Infinity.

			2.0 / 0.0;
			// Infinity
			// number / number; -> division
			// Is Infinity because anything divided by 0 automatically gives you Infinity.

			2.0 / -0.0;
			// -Infinity
			// number / number; -> division
			// Is -Infinity because anything divided by -0 automatically gives you -Infinity.

			2 * 2;
			// => 4
			// number * number; -> multiplication

			2 * -2;
			// => -4
			// number * number; -> multiplication
			// Is -4 because anything multiplied by a negative (-) automatically gives you a '-' numerical.

			Infinity * 0;
			// => NaN
			// number * number; -> multiplication

			Infinity * Infinity;
			// => Infinity
			// number * number; -> multiplication

			'foo' * 2;
			// => NaN
			// string * number; -> multiplication
			// Is NaN because a literal string is not a number.

			12 % 5
			// => 2
			// number % number; -> remainder
			// Is 2 because there are 2 fives in 12 with a remainder of 2.

			-1 % 2
			// => -1
			// number % number; -> remainder

			-1 % 5
			// => -1
			// number % number; -> remainder

			NaN % 2
			// NaN
			// number % number; -> remainder

			var x = 3;
			y = x++;
			// => y = 3, x = 4
			// Is y = 3 because 'var x = 3'. It was used postfix (x++), with operator (++) after operand (x), it will return the value 3 before incrementing.
			// Is x = 4 because the increment operator adds '1' to its operand (x) and returns a value; which 'var x = 3' + '1' equals 4.

			var a = 2;
			b = ++a; 
			// => a = 3, b = 3
			// Is a = 3 because it was used prefix (++x), with operator (++) before operand (x), it will return the value 3 after incrementing by 1.
			// Is b = 3 because after incrementing '++a', it will return the value 3.

		// 3.1.3. Expressions
		// Use the arithemtic operators (+, -, /, *) to several operators and values together.
			// Tip: Mathematical expressions are evaluated from left to right. 

			(2 + 3) * (9 - 8);
			// => 5
			// Is 5 because '(2 + 3) = 5' and '(9 - 8) = 1'; which updates to '(5) * (1)' and equals 5.

			((9 + 2) * 25) - 5;
			// => 270
			// Is 270 because '(9 + 2) = 11'; which updates to '((11) * 25)' and equals 275. Then '((275)) - 5 = 270'.

	// 3.2. Variables
		// 3.2.1. Expression with Variables

			var x = 10;
			x + 20;
			// => 30
			// 10 + 20; -> arithmetic

			var x = 10;
			x * x;
			// => 100
			// 10 x 10; -> arithmetic

			var x = 10;
			3 * (x * x) - 2 * x + 5;
			// => 285
			// 3 * (10 * 10) - 2 * 10 + 5; -> arithmetic
			// Is 285 because '3 * (10 * 10) = 300'; which updates to '300 - (2 * 10) + 5'. Then '300 - 20 + 5 = 285'.

			var x = 10;
				x = 1;
				x = 5;
				x = 15;
				2 * x;
			// => 30
			// Is 30 because the most recent value of x (15) is retrained; which updates to '2 * 15' and equals 30.

			var x = 5;
			var y = 10;
				x = y + 10;
			// => x = 20
			// x = 10 + 10;
			// Is x = 20 because 'y + 10 = 20'.

			var x = 1;
			var y = 10;
				x = y * 2; 
				y = x + 1;
				x = y + 1;
				y = 2 * x;
			// => y = 44, x = 22
			// Is y = 44 and x = 22 because the variables have been redefined.
				// x = 10 * 2; => x = 20
				// y = 20 + 1; => y = 21
				// x = 21 + 1; => x = 22
				// y = 2 * 22; => y = 44

			var x = 1;
			var y = 2;
			var z = 3;
				x = y;
				y = z;
				z = x;
			// => x = 2, y = 3, z = 2
			// Is x = 2, y = 3, and z = 2 because the variables have been redefined.
			// x = 2;
			// y = 3;
			// z = 2;

			var x = 1;
			var y = 0;
			var z = -1;
				x = y + z;
				y = z * x;
				z = x - y;
				x = y * y;
				y = z * z;
				z = z - 1;
			// => x = 1, y = 4, z = -3
			// Is x = 1, y = 4, amd z = -3 because the variables have been redefined.
			// x = 0 + -1; 	=> x = -1
			// y = -1 * -1; => y = 1
			// z = -1 - 1; 	=> z = -2
			// x = 1 * 1;	=> x = 1
			// y = -2 * -2; => y = 4
			// z = -2 - 1;	=> z = -3
	
	// 3.3. Boolean Logic
	// 3.4. Quiz Questions
		// 3.A. Evaluate the expression below. What will the result be?
			9 - 1 + 2 - 4;
			// => 6

		// 3.B. Evaluate the expression below. What will the result be?
			(30 - 5) * 2 / 5;
			// => 10
		
		// 3.C. Evaluate the bolded expression below. What will the result be?
			('The ' + 'rain ') + ('in Spain ' + 'stays ') + 'mainly ' + ('in ' + 'the ') + 'plain.';
			// => 'The rain in Spain stays mainly in the plain.'
		
		// 3.D. Evaluate the expression below. What will the result be?
			(true || false) && (true && false);
			// => fasle
		
		// 4.0. How do you create a variable called 'name' in Javascript?
			// var name = 'John';

		// 5.0. What is the final value of x?
			x = 0;
			x = x + 1;
			x = x + 2;
			x = x + 3;
			x = x + 4;
			x = x + 5;
			x = x + 6;
			x = x + 7;
			// => 28

		// 6.0. Declare a new variable called fullName that contains both John's first and last name, with a space between them.
			var firstName = 'John';
			var lastName = 'Smith';
			// var fullName = firstName + " " + lastName;
		
		// 7.0. What does this statement evaluate to?
			10 > 9 > 8 === true;
			// => false
			// Is false because according to JavaScript's order of operations, the line should be evaluated as: ((10 > 9) > 8); which evaluates to: (true > 8).
			// In this context (when using the > operator), booleans will be converted to numbers, so this will evaluate to: (1 > 8).
		
		// 8.0. Evaluate the last statement in the code below.
			var x;
			x === x;
			// => true
		
		// 9.0. Evaluate the last statement in the code below.
			var x;
			x === x;
			y === y;
			// => Undefined
			// If you were to run this code in the console, JavaScript's response would be ""ReferenceError: y is not defined".  
			// JavaScript has no idea what "y" is and would not be able to evaluate the statement.  So, the statement is undefined.

// End of Unit 3.