"use strict";
exports.__esModule = true;
var gulp_1 = require("gulp");
var task = function (seqTasks, init) {
    if (init === void 0) { init = false; }
    var tasks = [];
    var hasAsync = false;
    seqTasks.forEach(function (item) {
        if (Array.isArray(item)) {
            hasAsync = true;
            tasks.push(gulp_1.parallel.apply(void 0, task(item)));
        }
        else {
            tasks.push(item);
        }
    });
    return (hasAsync && !init) ? [gulp_1.series.apply(void 0, tasks)] : tasks;
};
function sequence() {
    var args = [];
    for (var _i = 0; _i < arguments.length; _i++) {
        args[_i] = arguments[_i];
    }
    return function (done) {
        gulp_1.series.apply(void 0, task(args, true))(done);
    };
}
exports.sequence = sequence;