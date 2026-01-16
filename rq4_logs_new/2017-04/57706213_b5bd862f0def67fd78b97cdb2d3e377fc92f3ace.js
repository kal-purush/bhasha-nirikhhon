
var path = require("path");
var fs = require("fs");

class FileSysDevice {
    constructor(dest) {
        this.dest = dest;
    }

    getSnapshotFilename(tag) {
        var filename = this.dest.pattern + tag + '.json';
        var filePath = path.join(this.dest.folder, filename);

        return filePath;
    }

    getSnapshot(tag, success, error) {
        var filePath = this.getSnapshotFilename(tag);

        console.log('Loading snapshot from ' + filePath);

        fs.access(filePath, fs.constants.R_OK, function (err) {
            if (err) {
                console.log('\tFile not found!');
                success(false);
            } else {

                fs.readFile(filePath, function (err, data) {
                    data = data.toString('utf8');
                    success(data);
                });
            }
        });
    }
    saveSnapshot(json, tag, success, error) {
        var filePath = this.getSnapshotFilename(tag);
        console.log('Saving JSON to "' + filePath + '"...');

        fs.writeFile(filePath, json, success);
    }
}

module.exports = FileSysDevice;