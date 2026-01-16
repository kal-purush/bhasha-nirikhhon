'use strict';

import log4js = require('log4js');
import fs = require('fs');

const LAYOUT_FILE = {
    "type": "pattern",
    "pattern": "%d [%-5p] - %m"
};

const LAYOUT_CONSOLE = {
    "type": "pattern",
    "pattern": "%d [%[%-5p%]] - %m"
};

const CONFIG = {
    appenders: [
        {
            'type': 'console',
            'layout': LAYOUT_CONSOLE
        },
        {
            'type': 'dateFile',
            'filename': 'logs/homeautomation.log',
            'pattern': '-yyyyMMdd',
            'alwaysIncludePattern': false,
            'layout': LAYOUT_FILE
        }
    ],
}

// create the log directory if not yet existing
let path: string = 'logs';
if (!fs.existsSync(path)) {
    fs.mkdirSync(path);
}

log4js.configure(CONFIG);

export let logger = log4js.getLogger();
logger.setLevel('trace');
