var fs = require('fs');

var bitMapr = {};

bitMapr.readBmp = function(path, callback) {
  fs.readFile(path, function(err, data) {
    if (err) throw err;
    var buf = Buffer.from(data);
    callback(null, buf);
  });
};



module.exports = bitMapr;