"use strict";
exports.__esModule = true;
var child_process_1 = require("child_process");
var semver = require("semver");
var chalk_1 = require("chalk");
exports.checkNpmVersion = function () {
    var hasMinNpm = false;
    var npmVersion = null;
    try {
        npmVersion = child_process_1.execSync('npm --version')
            .toString()
            .trim();
        hasMinNpm = semver.gte(npmVersion, '5.0.0');
        if (!hasMinNpm) {
            console.error("npm version higher as 5.0.0 is required. current version " + chalk_1["default"].red("\"" + npmVersion + "\""));
            process.exit(1);
        }
    }
    catch (err) {
        console.error(err);
        process.exit(1);
    }
};
exports.checkNodeVersion = function () {
    if (!semver.satisfies(process.version, "8")) {
        console.error(chalk_1["default"].red('You are running Node %s.\n' +
            'create sipgate app requires Node %s or higher. \n' +
            'Please update your version of Node.'), process.version, "8");
        process.exit(1);
    }
};
console.log("Check nodejs version ...");
exports.checkNodeVersion();
console.log("nodejs version", chalk_1["default"].green("OK"));
console.log("Check npm version ...");
exports.checkNpmVersion();
console.log("npm version", chalk_1["default"].green("OK"));
console.log("Build binary...");