"use strict";

const util = require('util');

let colorWarn = "";
let colorErr = "";
let colorReset = "";

/**
 * @type {ModuleOptions}
 */
const options = {
    timeStampFormatter: undefined,
    showLogLevel: false,
    inspectOptions: {},
    logInfoFn: console.log,
    logWarnFn: console.warn,
    logErrorFn: console.error,
    set logLevelColored(value) {
        // https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
        colorWarn = value ? "\x1b[93m" : ""; // When `value` is true: yellow
        colorErr = value ? "\x1b[91m" : "";  // When `value` is true: red
        colorReset = value ? "\x1b[0m" : ""; // When `value` is true: reset to terminal default
    },
    get logLevelColored() { return colorReset !== ""; }
}

/**
 * Global options for this module.
 * @type {ModuleOptions}
 */
exports.options = options;

/**
 * Extracts the module entrypoint's caller's location from the current callstack.
 * @param {Error} error The error object. Not used.
 * @param {Array<NodeJS.CallSite>} structuredStackTrace
 * @returns {string} The caller's location in the format: 'location/to/file:lineNumber'. Example: `/app/start.js:15`
 */
const getCallerLocationHelper = (error, structuredStackTrace) => {
    // Skip over getCallerLocation(), and the exports.(entry method)
    // to the 3rd element, the actual caller method's location.
    const caller = structuredStackTrace[2];
    return `${caller.getFileName()}:${caller.getLineNumber()}`
}

/**
 * Gets the external caller's location.
 * @returns {string} The caller's location in the format: 'location/to/file:lineNumber'. Example: `/app/start.js:15` or `c:\app\start.js:15`
 */
const getCallerLocation = () => {
    // Read more about Error.prepareStackTrace and CallSite here: https://v8.dev/docs/stack-trace-api

    const originalFormatter = Error.prepareStackTrace;
    Error.prepareStackTrace = getCallerLocationHelper;

    const stackObj = {};
    Error.captureStackTrace(stackObj);
    const result = stackObj.stack;

    Error.prepareStackTrace = originalFormatter;
    return result;
}

/**
 * Logs info message in the format: '{timestamp?} {INFO?} /path/to/file:lineNumber: rest of arguments'.
 * Set module `options` to control wether timestamp, and log level are shown.
 */
exports.logInfo = function () {
    options.logInfoFn(
        options.timeStampFormatter ? options.timeStampFormatter() : "",
        options.showLogLevel ? "INFO " : "",
        `${getCallerLocation()}:`,
        util.formatWithOptions.apply(null, [options.inspectOptions, ...arguments]));
}

/**
 * Logs warning message in the format: '{timestamp?} {WARN?} /path/to/file:lineNumber: rest of arguments'
 * Set module `options` to control wether timestamp, and log level are shown.
 */
exports.logWarn = function () {
    options.logWarnFn(
        options.timeStampFormatter ? options.timeStampFormatter() : "",
        options.showLogLevel ? colorWarn + "WARN " + colorReset : "",
        `${getCallerLocation()}:`,
        util.formatWithOptions.apply(null, [options.inspectOptions, ...arguments]));
}

/**
 * Logs error message in the format: '{timestamp?} { ERR?} /path/to/file:lineNumber: rest of arguments'
 * Set module `options` to control wether timestamp, and log level are shown.
 */
exports.logError = function () {
    options.logErrorFn(
        options.timeStampFormatter ? options.timeStampFormatter() : "",
        options.showLogLevel ? colorErr + "ERR  " + colorReset : "",
        `${getCallerLocation()}:`,
        util.formatWithOptions.apply(null, [options.inspectOptions, ...arguments]));
}

/**
 * Gets the timestamp.
 * @callback TimeStampFormatterFn
 * @returns {string} The timestamp.
 */
/**
 * Calls the logging backend.
 * @callback LoggerFn
 * @param {string} timestamp An optional timestamp string.
 * @param {string} logLevel An optional log level string.
 * @param {string} callSite A string in the format `/path/to/file:lineNumber:`
 * @param {...any} rest The rest of arguments.
 * @returns {void}
 */
/**
 * @typedef {Object} ModuleOptions
 * @prop {TimeStampFormatterFn|undefined} timeStampFormatter
 * When `undefined`, logs are not timestamped. When assigned to a method, it will be called for every log method.
 *
 * Examples:
 *
 * `timeStampFormatter = () => new Date().toISOString();`
 * `timeStampFormatter = () => new Date().toLocaleTimeString();`
 * @prop {boolean} showLogLevel When set to true, the log level is displayed in log messages.
 * @prop {util.InspectOptions} inspectOptions The options instance passed to `util.formatWithOptions`.
 * @prop {LoggerFn} logInfoFn The method to call when logging an informational message, set to `console.log` by default.
 * @prop {LoggerFn} logWarnFn: The method to call when logging a warning message, set to `console.warn` by default.
 * @prop {LoggerFn} logErrorFn: The method to call when logging an error message, set to `console.error` by default.
 * @prop {boolean} logLevelColored When set to true and `showLogLevel` is also true, WARN is colored as yellow, and ERR is colored as red.
 */