import childProcess from 'child_process';

import Command from '../command.js';
import log from '../util/logger.js';

class child extends Command {
    constructor() {
        super();
    }

    get name() {
        return '*';
    }

    apply(args, opt) {
        let childStream = childProcess.spawn(opt.command, args, {
            stdio: 'pipe'
        }).on('error', (err) => {
            log.v('child: errCode ' + err.code);
            if (err.code === 'ENOENT') {
                console.log('Command not found: ' + opt.command + '\n');
            } else {
                console.log('OI!');
                console.log(err);
            }
        });

        childStream.on('close', (code) => {
            log.d(`child process exited with code ${code}`);
        });
        return childStream.stdout;
    }
}

export default child;