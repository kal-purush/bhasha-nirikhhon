import * as http from 'http';
import * as net from 'net';
import * as child_process from 'child_process';
import * as Promise from 'bluebird';
import * as path from 'path';

const getPort = (): Promise<any> => {
    return new Promise((resolve, reject) => {
        const server = http.createServer();
        server.listen(0);
        server.on('listening', () => {
            const port = server.address().port;
            server.close();
            resolve(port); 
        });
    });
};

export const newVisualProcess = (dataId: string): Promise<any> => {
    const cpPath = path.join(__dirname, 'UDX.visualization.controller.ts');
    return new Promise((resolve, reject) => {
        getPort()
            .then(port => {
                const cp = child_process.fork(cpPath, [], {
                    execArgv: ['--debug='+port]
                });
                cp.send({
                    code: ''
                });
                cp.on('message', m => {
                    if(m.code === 'kill') {
                        cp.kill();
                        return resolve(m.data);
                    }
                });
            })
            .catch(reject);
    })
};