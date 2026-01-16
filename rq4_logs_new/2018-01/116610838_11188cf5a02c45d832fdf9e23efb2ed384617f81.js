/*

A Pythagorean triplet is a set of three natural numbers, a < b < c, for which,
a2 + b2 = c2

For example, 32 + 42 = 9 + 16 = 25 = 52.

There exists exactly one Pythagorean triplet for which a + b + c = 1000.
Find the product abc.

*/

function pythagoreanTriplet(product){
    let sum;
    
    while(sum !== 1000) {
        for(a = 0; a < 1000; a++){
            for(b = 0; b < 1000; b++){
                for(c = 0; c < 1000; c++){
                    if(a*a + b*b === c*c) {
                        sum = a + b + c
                        console.log(sum)
                    }
                }
            }
        }
    }
    return [a,b,c]
}

console.log(pythagoreanTriplet(1000))