import * as express from "express";
import { Application } from "express";

class App {
    public app: Application;
    public port: number;

    constructor(port: number = 3000) {
        this.port = port;
        this.app = express();

        this.middlewares();
        this.routes();
    }

    public listen() {
        this.app.listen(this.port, () => {
            // tslint:disable-next-line
            console.log(`App listening on the http://localhost:${this.port}`);
        });
    }

    private middlewares() {
        const middleWares = [];

        middleWares.forEach((middleWare) => {
            this.app.use(middleWare);
        });
    }

    private routes() {
        const controllers = [];

        controllers.forEach((controller) => {
            this.app.use("/", controller.router);
        });
    }
}

export default App;