import express, { Express } from 'express';
import App from '../app';

interface IServer {
    start: (afterStart?: () => unknown) => void;
}

class Server implements IServer {
    protected app: App;

    protected express: Express;

    /**
     * @param app App
     */
    constructor(app: App) {
        this.app = app;
        this.express = express();
    }

    /**
     * Instantiate a new Server class.
     * @param app App
     * @returns new Server()
     */
    static init(app: App): Server {
        return new this(app);
    }

    /**
     * Listen on server configuration and run any callback if exists.
     * @param afterStart afterStart () => unknown
     */
    start(afterStart?: () => unknown): void {
        const port = this.app.config('app.host.port', 9004);
        const server = this.app.config('app.host.ip', '0.0.0.0');
        const environment = this.app.config('app.env', 'development');

        this.express.listen(Number(port), String(server), () => {
            console.log(`[${environment}] - Server running @ http://${server}:${port}`);
            if (afterStart) afterStart();
        });
    }
}

export default Server;