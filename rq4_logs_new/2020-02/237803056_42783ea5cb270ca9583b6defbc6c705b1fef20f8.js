const fs = require('fs');
const path = require('path');

if (__dirname.split(/\\|\//g).pop() !== '~') {
  throw new Error(`This package must be installed with an alias of "~". Try installing this as:
npm install ~@kolodny/squigly --save-dev
  Don't forget to remove the non aliased squigly from the package.json!`);
}

for (const file of fs.readdirSync(__dirname)) {
  fs.unlinkSync(__dirname + '/' + file);
}
fs.rmdirSync(__dirname);
fs.symlinkSync(path.resolve(__dirname + '/..'), __dirname);
console.log(`~ now points to the package root!`);