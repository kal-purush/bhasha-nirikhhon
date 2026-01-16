
import {Tiger, TigerMethod, TigerModule, State} from "./types"

import express = require("express");
import log4js = require("log4js")
import fs = require("fs")

const DEFAULT_SERVER_PORT = 9527;

const LOGGER: log4js.Logger = log4js.getLogger("TigerServer");
LOGGER.level = "info";

export default class TigerServer implements Tiger {

    server: express.Express;

    triggers: { [key: string]: TigerModule } = {}

    db: { [key: string]: State } = {}

    serverPort?: number;

    constructor() {
        LOGGER.info("Creating a new TigerServer instance");
        this.server = express();
    }

    serve(basePath: string) {
        LOGGER.info(`Served in path: ${basePath}`);
        
        fs.readdir(basePath, (err, files) => {
            files.forEach(file => {
                LOGGER.info(`Preload module: [${file}]`)
                this.loadModules(basePath, file);
            });
        })

        // TODO: changed to fs.watch
        this.server.post("/loader", (request, response) => {
            let {path} = request.body;
            let {force}  = request.params;

            LOGGER.info(`Manually load modules ${path} with force options?: ${force}`)
            
            let result = this.loadModules(basePath, path, force);

            response.send(result);
        });

        this.server.get("/state/:module", (request, response) => {
            response.send(this.state(request.params["module"]));
        });

        this.server.listen(this.serverPort || DEFAULT_SERVER_PORT);
    }

    config(configurer: (express: express.Express) => void) {
        configurer(this.server);
    }

    port(port: number) {
        this.serverPort = port;
    }

    private state(moduleName: string, value?: State): State | undefined {
        if (value) {
            this.db[moduleName] = value
        }
        return this.db[moduleName]
    }

    private loadModules(basePath: string, path: string, force?: boolean): { status: boolean, path: string } {
        let status = true;
        let servedPath = `/modules/${path}`;
        try {
            let mod: TigerModule = require(`${basePath}/${path}`);
            
            this.triggers[servedPath] = mod;

            if (force || !this.state(path)) {
                this.state(path, mod.state || {});
            }

            this.server[mod.method](servedPath, (request, response) => {
                let state = this.state(path);
                mod.handler(request, response, state);
                this.state(path, state);
            });
        } catch (e) {
            status = false;
        }

        return { status, path: servedPath }
    }
    
}