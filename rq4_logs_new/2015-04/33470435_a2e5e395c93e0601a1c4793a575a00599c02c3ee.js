var cheerio = require('cheerio');
var http = require('http');
var parser = require('./htmlParser');

function getLineRealTimeInfo(lineId, callBack) {
    var options = {
        host: 'www.szjt.gov.cn',
        port: 80,
        path: '/apts/APTSLine.aspx?lineGuid=' + lineId
    };
    var html = '';
    http.get(options, function (res) {
        res.on('data', function (data) {
            html += data;
        }).on('end', function () {
            var $ = cheerio.load(html);
            var tds = $(".main td");
            var len = tds.length;
            var arr = [];
            var obj;
            for (var i = 0; i < len; i += 4) {
                obj = {};
                obj.name = parser.getNodeText(tds[i]);
                obj.bus = parser.getNodeText(tds[i + 2]);
                obj.time = parser.getNodeText(tds[i + 3]);
                arr.push(obj);
            }
            callBack(arr);
        });
    });
}

function getLineInfo(lineNum, callBack) {
    var resultArr = [];

    var postData = '__VIEWSTATE=%2FwEPDwUJNDk3MjU2MjgyD2QWAmYPZBYCAgMPZBYCAgEPZBYCAgYPDxYCHgdWaXNpYmxlaGRkZNSq3M6FLiH9uhezHaCZYWZQT%2F9VbteYCJ3RkqvkKG66'
        + '&__VIEWSTATEGENERATOR=964EC381'
        + '&__EVENTVALIDATION=%2FwEWAwLZi%2B7ABAL88Oh8AqX89aoKVJoV4zpUp4emFejE9%2F7pXtFgzQ3x2PHjaMh9lXvOycg%3D'
        + '&ctl00%24MainContent%24LineName=' + lineNum
        + '&ctl00%24MainContent%24SearchLine=%E6%90%9C%E7%B4%A2';

    var options = {
        host: 'www.szjt.gov.cn',
        port: 80,
        method: 'POST',
        path: '/apts/APTSLine.aspx',
        headers: {
            'Content-Length': postData.length,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
    };

    var html = '';
    var req = http.request(options, function (res) {
        res.setEncoding('utf8');
        res.on('data', function (data) {
            html += data;
        }).on('end', function () {
            var Line = global.models.Line;
            var line;
            var lineObj;
            var $ = cheerio.load(html);
            var tds = $(".main td");
            for (var i = 0, len = tds.length; i < len; i += 2) {
                line = parser.getNodeText(tds[i]);
                lineObj = new Line();
                lineObj.lineId = parser.getLineId(tds[i]);
                lineObj.line = line;
                lineObj.info = parser.getNodeText(tds[i + 1]);
                resultArr.push(lineObj);
            }
            callBack(resultArr);
        });
    });
    req.write(postData);
    req.end();
}

exports.getLineRealTimeInfo = getLineRealTimeInfo;
exports.getLineInfo = getLineInfo;