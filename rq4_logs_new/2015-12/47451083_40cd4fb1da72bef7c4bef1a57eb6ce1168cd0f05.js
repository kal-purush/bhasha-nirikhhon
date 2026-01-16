
var arr = require('../index');
var s = arr.getNickname();
if (s) {
    console.log(s);
} else {
    throw new Error('The test does not pass...');
}