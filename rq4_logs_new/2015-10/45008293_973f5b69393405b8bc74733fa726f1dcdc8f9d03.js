var fs = require('fs');
var path = require('path');

var target = 'emojis';

fs.mkdirSync(path.resolve(__dirname, target));