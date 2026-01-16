var https = require('https');
var Promise = require('promise');
var cheerio = require('cheerio');
var async = require('async');

var url = "https://uspdigital.usp.br/jupiterweb/listarGradeCurricular?codcg=3&codcur=3021&codhab=100&tipo=N";

simpleHttpsGet(url, function(error, data) {
    console.log(data);

});



function simpleHttpsGet(url, callback) {

    var recData = '';

    https.get(url, function(res) {
        res.setEncoding('utf8');

        res.on('data', function(chunk) {
            recData += chunk;
        });

        res.on('end', function() {
            callback(null, recData);
        });

        res.on('error', function(e) {
            callback(e);		
        });

    }).on('error', (e) => {
        callback(e);
    });
}