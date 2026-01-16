/**
*	Trial Division algorithm for factorization
*
*	@author Lakha Singh
*/

// Read Input from console
var readInput = function( callback ){
	process.stdin.setEncoding('utf8');
	callback = callback || function(){};

	// Start Listening to user input
	process.stdin.addListener('data', function( data ){
		callback( data );
	});
};

// To find if given number is prime
var isPrime = function( num ){
	var limit = parseInt( Math.sqrt( num ), 10);

	var i = 2, k = 1;

	// As 2 and 3 cant be included in 6k+1, hence checking them seperately
	for ( ; i <= 3; i++ ){
		if ( num % i === 0 ){
			return false;
		}
	}

	// For optimzation, checking against only prime divisors
	while ( i <= limit ){
		for ( j = -1; j <= 1; j +=2 ){
			i = 6*k + j;

			if ( num % i === 0 ){
				return false;
			}
		}
		k++;
	}
	return true;
};

// Start
readInput(function( input ){
	console.log( 'This number is ' + (isPrime( parseInt( input, 10 ) ) ? 'Prime' : 'Not a Prime'));
	process.exit();
});