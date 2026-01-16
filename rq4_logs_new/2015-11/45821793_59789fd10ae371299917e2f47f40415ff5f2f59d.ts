// IMPORTS
// ============================================================================
import * as fs from 'fs';
import * as path from 'path';

// MODULE VARIABLES
// ============================================================================
var settings: Settings;
var configDir: string = 'config';

var DEFAULTS = {
    port: process.env.PORT || 3000,
    env: process.env.NODE_ENV || 'development'
}

// INTERFACE DEFINITIONS
// ============================================================================
export interface Settings {
    port: number;
    env: string;
}

// EXPORTED FUNCTIONS
// ============================================================================
export function getSettings() : Settings {

    // if config settings have already been read, just return them
    if (settings) return settings;

    // otherwise, read remaining settings from the configuration file
    try {
        var file: string = path.join(process.cwd(), configDir, DEFAULTS.env) + '.json';
        console.info('Reading configuration from ' + file);
        var obj = JSON.parse(fs.readFileSync(file, 'utf8').toString());
    }
    catch (err) {
        err.message = 'Failed to read config file: ' + err.message;
        throw err;
    }

    // create new settings object
    settings = Object.assign({}, DEFAULTS, obj);
    return settings;
}

export function getDefaults() : Settings {
    return DEFAULTS;
}