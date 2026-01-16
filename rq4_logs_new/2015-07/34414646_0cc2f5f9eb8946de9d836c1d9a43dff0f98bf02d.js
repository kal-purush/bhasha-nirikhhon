function is_prime(n){
    
    if (n == 1 || n%2 == 0 || n%3 == 0) return false;
    if (n != Math.round(n)) return false;
    
    var isPrime = true;
    
    for (var i = 2; i <= Math.sqrt(n); i++){
        if (n % i == 0) isPrime = false;
    }
    
    return isPrime;
}

function largest_prime_factor(n){
    var roof = Math.floor(Math.sqrt(n));
    for (var i = roof; i > 3; i-- ){
        if (n%i == 0 && is_prime(i)){
            return i;
        }
    }
}

console.log(largest_prime_factor(600851475143));