/* eslint-disable no-console */
import * as childProcess from 'child_process';

export interface ExecResult {
    stdout: string,
    stderr: string,
}

export const exec = async (
    command: string,
    options: childProcess.ExecOptions = {},
): Promise<ExecResult> => await new Promise<ExecResult>((resolve, reject) => {
    childProcess.exec(command, options, (error, stdout, stderr) => {
        if (error) {
            console.info(`--- stdout ---\n${stdout}`);
            console.info(`--- stderr ---\n${stderr}`);
            reject(error);
        } else {
            resolve({
                stdout: stdout.trim(),
                stderr: stderr.trim(),
            });
        }
    });
});