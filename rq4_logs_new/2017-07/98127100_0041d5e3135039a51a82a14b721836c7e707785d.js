// Exercises found on https://sites.google.com/site/prologsite/prolog-problems
// 2. Arithmethic

// 2.1 Determine whether a given integer number is prime.
	/*
	 	Example:
		isPrime(7).
		result = true
	*/

	//Definition of a prime: A prime number (or a prime) is a natural number greater than 1 that has no positive divisors other than 1 and itself. (wikipedia)

function isPrime(number) {
	var truthList = [];
	for (var i = 2; i <= number; i++) {
		if(Number.isInteger(number/i)) {
			truthList.push(true)
		}
		if(truthList.length > 2) {
			console.log('FALSE - This is NOT a Prime number')
			return false
		}
	}
	console.log('TRUE - This is a Prime number')
	return true
}

isPrime(7)

// 2.2 Determine the prime factors of a given positive integer.
	/*
	 	Example:
		primeFactorList(315, L).
		result = true
	*/

function primeFactorList(number) {
	var factors = [];
	//find divisors starting with 2
	for (var i = 2; i <= number; i++) {
		while ((number % i) === 0) {
            factors.push(i);
            //console.log(i, 'i')
            //console.log(number /= i, 'number /= i')
        	// Q===========> WHY do i get [3,3,5,7]. why is 3 twice in this list. Also, if I uncomment the two console logs, why does it only display [3,5]
            number /= i;
        }
	}
	console.log('the prime factors in ascending order are:', factors)
}

primeFactorList(315)