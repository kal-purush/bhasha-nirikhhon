///<reference path="typings/tsd.d.ts"/>

import util = require('util');

// Default Levels
// 0: error
// 1: info
// 2: debug
var levels = [
    'error', 'info', 'debug'
];

export interface LogOptions {
    level?: string;
    pretty?: boolean;
}

interface LogMessage {
    level: number;
    item: any;
}

export class Log {
    level: string = 'debug';
    opts: LogOptions = {};

    constructor(opts?: LogOptions) {
        this.opts = opts;
        this.setLevel();
    }

    write(msg: LogMessage) {
        console.log(this.level + ':' + msg.item);
    }

    lvlNum(): number {
        return levels.indexOf(this.level) || 0;
    }

    debug(...any) {
        for (var i = 0; i < arguments.length; i++) {
            this.write({
                level: this.lvlNum(),
                item: arguments[i]
            });
        }
    }

    setLevel(lvl?: string) {
        // Allow manual setting of the level
        if (lvl) {
            return this.level = lvl;
        }
        // Get level from environment
        var env = process.env['LOG_LEVEL'];
        if (env) {
            return this.level = env;
        }
        // Get level from this logger's options
        if (this.opts.level) {
            return this.level = this.opts.level;
        }
    }

}