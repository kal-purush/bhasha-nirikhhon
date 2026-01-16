// Trivial Factorial Benchmark -- JavaScript Version
// This code is in the public domain
//
// BROKEN: allocates memory without end...

var bignum = require('bignum');

function fact(n) {
  var acc = bignum(1);
  for (var i=1; i<=n; i++) {
    acc = acc.mul(i);
  }
  return acc;
}
console.log(fact(200000).tostring());