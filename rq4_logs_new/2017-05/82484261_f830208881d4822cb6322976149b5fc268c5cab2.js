var base = parseInt(prompt('Enter the base: '));
var power = parseInt(prompt('Enter the power: '));
var result = 1;

function calcPow(base, power) {
  if (power > 0) {
    for (i = 1; i <= power; i++) {
    result = result * base;
  }
} else if (power < 0) {
  power = -power;
    for (i = 1; i <= power; i++) {
      result = result * base;
    }
    result = 1 / result;
  }

  return result;
}

console.log('calcPow result is: ' + base + ' ^ ' + power + ' = ' + calcPow(base, power));
console.log('mathPow result is: ' + Math.pow(base, power));