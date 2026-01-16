'use strict';

var chalk = require('chalk');

// Based on http://blog.codehatcher.com/node-js-alternate-config-file
module.exports = (function () {
    var pkg = require('package'),
        YAML = require('yamljs'),
        argv = require('yargs').argv,
        dataDir = argv.dataDir || 'data',
        // [todo] - The three below I would love to break out
        siteConfig =  YAML.load(dataDir + '/site.yaml'),
        characterConfig =  YAML.load(dataDir + '/characters.yaml'),
        mediaConfig =  {},
        error = chalk.bold.red,
        warning = chalk.yellow,
        info = chalk.cyan,
        debug = chalk.blue,
        success = chalk.bold;

    switch (process.env.NODE_ENV) {
        case null:
        case undefined:
        case 'local':
            return {
                env: 'local',
                site: siteConfig,
                character: characterConfig,
                media: mediaConfig,
                dataDir: dataDir,
                pkg: pkg,
                error: error,
                warning: warning,
                info: info,
                debug: debug,
                success: success,
                gtm: 'GTM-N2JHDQ' // [todo] - Move to site config
            };
        case 'prod':
        case 'production':
            return {
                env: 'prod',
                site: siteConfig,
                character: characterConfig,
                media: mediaConfig,
                dataDir: dataDir,
                pkg: pkg,
                error: error,
                warning: warning,
                info: info,
                debug: debug,
                success: success,
                gtm: 'GTM-TK3LN4'
            };
        default:
            throw new Error('Environment Not Recognized');
    }
}());