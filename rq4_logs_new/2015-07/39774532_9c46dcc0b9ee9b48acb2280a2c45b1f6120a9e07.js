var fs = require('fs'); // this engine requires the fs module
var underscore = require('underscore');

exports.renderFile = function (filePath, options, callback) { // define the template engine
  fs.readFile(filePath, function (err, content) {
    if (err) return callback(new Error(err));
    
    var template = underscore.template(content.toString())    
    return callback(null, template(options));
  })
}