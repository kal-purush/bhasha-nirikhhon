var fs = require('fs');
var insertCss = require('insert-css');
var css = fs.readFileSync(__dirname + '/press-start.css');

module.exports = function () {
    insertCss(css);
};