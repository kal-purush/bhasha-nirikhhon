'use strict';
const readExcel = require('co-excel');
const co = require('co');
const request = require('request');
const file_write = require('write-file-atomic');
const writeFile = require('write-file-stdout');

const req = function (url) {
    return new Promise((resolve, reject) => {
        request.get(url,{timeout: 1500}, function (error, response, body) {
            if (!error && response.statusCode == 200) {
                resolve(true);
            }else{
                resolve(false);
            }
        })
    })
};

co(function *() {
    var excel = yield readExcel('1.xlsx');
    console.log(excel.length);
    var excelResult = yield excel.map(co.wrap(function *(data) {
        data[7] = yield req(data[2]);
        data[3] = " ";
        data[4] = " ";
        return (data.join(', ') + '\n');
    }));
    console.log(excelResult.length);
    yield writeFile('result.txt', excelResult);

});
