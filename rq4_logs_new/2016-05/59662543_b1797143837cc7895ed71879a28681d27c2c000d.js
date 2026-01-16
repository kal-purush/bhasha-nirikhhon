var csv = require('csv-parser');
var fs = require('fs');
var path = require('path');
var rmdir = require('rmdir');
var dateFormat = require('dateformat');
var sanitize = require("sanitize-filename");
var unidecode = require('unidecode');

var outputDir = 'output';
var filename = 'article.txt';
var fieldSeparator = '----\n';
var headers = ['Date', 'Title', 'Teaser', 'Text'];
var headerSeparator = ';';
var dispDateFormat = "yyyy-mm-dd";

function removeOutput(cb, csvFile, columnCount) {
    rmdir(outputDir, function(err, dirs, files){
        fs.mkdir(outputDir);
        cb(csvFile, columnCount);
    });
}

function generateHeader(headerList) {
    if (headerList) {
        headers = headerList.split(',');
    }
}

function parseCsv(csvFile, columnCount) {
    generateHeader(columnCount);
    var count = 0;
    fs.createReadStream(csvFile)
    .pipe(csv({
        separator: headerSeparator,
        headers: headers
        }))
    .on('data', function(data) {
        count++;
        console.log('Dispatched count: ' + count);
        parseRow(count, data);
    });
}

function getFolderName(outputDir, counter, rawDirBase) {
    var dirBase = sanitize(rawDirBase);
    dirBase = unidecode(dirBase);
    dirBase = dirBase.replace(/\s+/g, '-');
    return path.join(outputDir, counter + '-' + dirBase);
}

function unixTimestamp2jsDate(unixTs) {
    return new Date(unixTs *1000)
}

function formatDate(date) {
    return dateFormat(date, dispDateFormat);
}

function formatRowData(row, i, fieldName, needFieldSeparator) {
    var value = row[fieldName].replace(/^["'](.+)["'']$/,'$1');
    var content = fieldName + ": " + value + '\n';
    if (needFieldSeparator) {
        content += fieldSeparator;
    }
    return content;
}

function parseRow(counter, row) {
    var lastHeaderIndex = headers.length-1;
    row[headers[0]] = formatDate(unixTimestamp2jsDate(row[headers[0]]));
    var dir = getFolderName(outputDir, counter, row[headers[1]]);
    fs.mkdir(dir, function(err){
        if (err) {
            console.log("Failed to create folder: " + dir);
            console.log(err);
        } else {
            var file = path.join(dir, filename);
            var content = '';
            for (var i = 0; i < headers.length; i++) {
                content += formatRowData(row, i, headers[i], (i < lastHeaderIndex));
            }
            fs.writeFile(file, content, function(err){
                if (err) {
                    console.log("Failed to create file: " + file);
                    console.log(err);
                }
            });
        }
    });

}

if (process.argv[2]) {
    removeOutput(parseCsv, process.argv[2], process.argv[3]);
} else {
    console.log('Usage: node index.js <csvFile> [comma separated header]');
    console.log('if no "comma separated header" then will use the default headers: ' + headers);
}