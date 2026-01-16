/**
 * RSSParser
 */

var xml2js =  require('xml2js');
var parser = new xml2js.Parser();
var fs     =  require('fs');
var request = require('request');

var source = 'http://www.uisdc.com/feed';


function parseRSS (source, callback) {
    request(source, function (err, resp, body) {

        if (err) {
            console.warn('request failed');
            return callback(err);
        }

        if (resp.statusCode == 200) {

            parser.parseString(body, function (err, result) {
                if (err) {
                    console.warn('xmlParser error');
                    return callback(err);
                }
                var jsonResult = result; // JSON.stringify(result);
                //fs.appendFile('./ss.txt', jsonResult, function (err) {
                //    if (err) return;
                //});
                console.log(jsonResult);
                return callback(null, jsonResult);
                //console.log(result);
            });


        } else {
            console.warn('fail statusCode：' + resp.statusCode);
            return callback(err);
        }
    });
}

module.exports = parseRSS;
