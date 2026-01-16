import { spawn } from 'child_process';
import { Writable } from 'stream';

export type Probe = (data: string) => boolean;

export type Termination = () => void;

export interface Flow {
    path?: string;
    userDir?: string;
    port?: number;
    env?: { [key: string]: string };
    settings?: string;
    readinessProbe?: Probe;
    stderr?: Writable;
    stdout?: Writable;
}

export async function start(f: Flow = {}): Promise<Termination> {
    return new Promise((resolve, reject) => {
        const {
            path = 'flows.json',
            port = 1880,
            userDir = '.',
            env = {},
            settings = 'settings.js',
            stderr,
            stdout,
            readinessProbe,
        } = f;
        const args = [
            'node-red',
            '-p',
            `${port}`,
            '--userDir',
            userDir,
            '--settings',
            settings,
            path,
        ];

        const proc = spawn(`npx`, args, {
            env: {
                ...process.env,
                ...env,
            },
        });

        proc.on('error', reject);

        if (stderr) {
            proc.stderr.pipe(stderr);
        }

        if (stdout) {
            proc.stdout.pipe(stdout);
        }

        const termination = () => proc.kill();
        const probeWrapper = chunk => {
            const str = String(chunk);
            let resolved = false;

            if (readinessProbe && readinessProbe(str)) {
                resolve(termination);
                resolved = true;
            } else if (str.indexOf('Started flows') > -1) {
                resolve(termination);
                resolved = true;
            }

            if (resolved) {
                proc.stdout.off('dta', probeWrapper);
            }
        };

        proc.stdout.on('data', probeWrapper);
    });
}