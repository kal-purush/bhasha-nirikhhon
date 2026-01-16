
var through2 = require('through2');
var _ = require('lodash');

module.exports.transformWith = function (template) {
    var buffer = Buffer.from('');

    return through2(function (chunk, encoding, callback) {
        buffer = Buffer.concat([buffer, chunk]);
        callback();
    }, function (callback) {
        var data = buffer.toString();
        var parsedData = JSON.parse(data);

        var compiledTemplate = _.template(template);
        var output = compiledTemplate(parsedData);

        this.push(output);
        callback();
    });
};