import { expect } from 'chai';
import fs from 'fs';
import { tmpdir } from 'os';
import path from 'path';
import { Termination } from '../common/termination';
import { start } from './process';

describe('Node-Red flow', () => {
    it('should start', async () => {
        const baseDir = tmpdir();
        const settingsFile = path.join(baseDir, 'settings.js');

        fs.writeFileSync(
            settingsFile,
            `
            module.exports = {};
        `,
        );

        let err: Error | undefined;
        let terminate: Termination | undefined;

        try {
            terminate = await start({
                userDir: baseDir,
                settings: settingsFile,
                stderr: process.stderr,
                stdout: process.stdout,
            });
        } catch (e) {
            err = e;
        }

        if (!err && terminate) {
            await terminate();
        }

        fs.unlinkSync(settingsFile);

        expect(err).to.be.undefined;
        expect(terminate).to.be.a('function');
    });
});