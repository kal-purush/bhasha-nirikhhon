import fs = require('fs');
var Csv = require('csv-stringify');
import dl = require('daylight');
import q = require('q');

export function ExportToCsv(days: dl.Daylight[], csvFilePath: string): q.Promise<string> {

    var deferred = q.defer<string>();

    Csv(days, { header: true, rowDelimiter: 'windows' }, (err, value) => {
        if (err) deferred.reject(err);
        else {
            fs.writeFile(csvFilePath, value, err => {
                if (err) deferred.reject(err);
                else deferred.resolve(csvFilePath);
            });
        }
    });

    return deferred.promise;
}