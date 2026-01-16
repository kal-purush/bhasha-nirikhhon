
import express = require("express");
import log4js = require("log4js")

import fs = require("fs")

export type State = { [key: string]: State | any };

export type TriggerMethods = "get" | "put" | "post" | "delete" | "options" | "head" | "patch" | "all";

export interface TriggerModule {
    method: TriggerMethods,
    state: State,
    handler: (req: express.Request, res: express.Response, state?: State) => boolean
}

export interface Tiger {
    serve(basePath: string): void
    config(configurer: (express: express.Express) => void): void
}

const LOGGER: log4js.Logger = log4js.getLogger("TigerServer");
LOGGER.level = "info";

class TigerServer implements Tiger {


    server: express.Express;

    triggers: { [key: string]: TriggerModule } = {}

    db: { [key: string]: State } = {}

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
            
            let result = this.loadModules(basePath, path);

            response.send(result);
        });

        this.server.get("/state/:module", (request, response) => {
            response.send(this.state(request.params["module"]));
        });

        this.server.listen(9527)
    }

    config(configurer: (express: express.Express) => void) {
        configurer(this.server);
    }

    private state(moduleName: string, value?: State): State | undefined {
        if (value) {
            this.db[moduleName] = value
        }
        return this.db[moduleName]
    }

    private loadModules(basePath: string, path: string): { status: boolean, path: string } {
        let status = true;
        let servedPath = `/modules/${path}`;
        try {
            let mod: TriggerModule = require(`${basePath}/${path}`);
            
            this.triggers[servedPath] = mod;

            if (!this.state(path)) {
                this.state(path, mod.state);
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

export function tiger(fn?: (tiger: Tiger) => void): Tiger {
    let tiger = new TigerServer();
    if (fn) fn(tiger);
    return tiger;
}
