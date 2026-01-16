import * as debug from 'debug';
import * as express from 'express';
import * as http from 'http';
import * as https from 'https';
import {readFileSync} from 'fs';
import {resolve} from 'app-root-path';
import app from '../app';

const debugServer: debug.Debugger = debug('twitchr:server');
const portHttp: number = process.env.PORT_HTTP || 8080;
const portHttps: number = process.env.PORT_HTTPS || 8443;

const appHttp: express.Express = process.env.USE_TLS ? express() : app;

if (process.env.USE_TLS) {
    const options: https.ServerOptions = {
        cert: readFileSync(resolve('./cert/cert.pem')),
        key: readFileSync(resolve('./cert/key.pem')),
    };

    app.set('port', portHttps);
    const serverHttps: https.Server = https.createServer(options, app);
    serverHttps.listen(portHttps);

    appHttp.get('*', (req: express.Request, res: express.Response) => {
        res.redirect(301, `https://${req.hostname}:${portHttps + req.originalUrl}`);
    });

    serverHttps.on('error', onError);
    serverHttps.on('listening', onListening);
}

appHttp.set('port', portHttp);
const serverHttp: http.Server = http.createServer(appHttp);
serverHttp.listen(portHttp);

serverHttp.on('error', onError);
serverHttp.on('listening', onListening);

function onError(error: any): void {
    const port: number = process.env.USE_TLS ? portHttps : portHttp;
    const protocol: string = process.env.USE_TLS ? 'HTTPS' : 'HTTP';

    if (error.syscall !== 'listen') {
        throw error;
    }

    switch (error.code) {
        case 'EACCES':
            console.error(`${protocol} port ${port} requires elevated privileges`);
            process.exit(1);
            break;

        case 'EADDRINUSE':
            console.error(`${protocol} port ${port} is already in use`);
            process.exit(1);
            break;

        default:
            throw error;
    }
}

function onListening(): void {
    const port: number = process.env.USE_TLS ? portHttps : portHttp;
    const protocol: string = process.env.USE_TLS ? 'HTTPS' : 'HTTP';

    debugServer(`${protocol} server listening on port ${port}`);
}