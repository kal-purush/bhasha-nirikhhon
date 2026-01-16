#!/usr/bin/env node

/**
 * Created by edelacruz on 3/2/2015.
 * @requires program: svn to be on the system path
 */

'use strict';

require('sjljs');

var fs = require('fs'),
    path = require('path'),
    exec = require('child_process').exec,
    dargs = require('dargs'),
    argv = require('yargs').argv,
    log = console.log;

module.exports = (function () {

// Sudo code
    // If not a valid call (check passed in params)
        // Print help message(s)
        // Exit process

    // Compile svn options to a concatenated string

    // Compose command string

    // Debugging
    log(argv);

    var svnlog_process = exec('svn log http://svn-root-here.com/ --limit 3', function (err, stdout, stderr) {

        if (!sjl.empty(err)) {
            log(err);
        }

        if (!sjl.empty(stderr)) {
            log(stderr);
        }

        if (!sjl.empty(stdout)) {
            log(stdout);
        }
    });

}());