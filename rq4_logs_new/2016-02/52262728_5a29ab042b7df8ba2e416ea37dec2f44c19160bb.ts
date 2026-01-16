import Counter = require('./counter');

var c = new Counter();

console.log("Excel like counter; convert numbers to alphabetic characters like Excel do in columns.");
console.log("\n- regular version -");

console.log("  13 = " + c.convertRegular(13));
console.log(" 133 = " + c.convertRegular(133));
console.log("1333 = " + c.convertRegular(1333));
console.log("1493 = " + c.convertRegular(1493));

console.log("\n- recursion version -");

console.log("  13 = " + c.convertRecursion(13));
console.log(" 133 = " + c.convertRecursion(133));
console.log("1333 = " + c.convertRecursion(1333));
console.log("1493 = " + c.convertRecursion(1493));