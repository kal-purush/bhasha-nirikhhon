function factorial(num) {
    num = parseInt(num);
    if(isNaN(num)) throw new Error('Not a number');

    if(num <= 1) return 1;

    return num * factorial(num-1);
}

module.exports = factorial;