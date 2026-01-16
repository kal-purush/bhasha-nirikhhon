/**
 * Created by darwinmorales on 28/03/2016.
 */

function factorial(n) {
    return fact_iter(1, 1, n);
}

function fact_iter(product, counter, maxCount) {
    if (counter  > maxCount) {
        return product;
    } else {
        return fact_iter(counter * product, counter + 1, maxCount);
    }
}

console.log(factorial(100));