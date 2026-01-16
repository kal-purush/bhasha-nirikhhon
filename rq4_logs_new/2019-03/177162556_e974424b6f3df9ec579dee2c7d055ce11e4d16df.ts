import BodyParser from "body-parser";
import Compression from "compression";
import express from "express";
import Helmet from "helmet";
import {inject} from "inversify";
import {provide} from "inversify-binding-decorators";
import {Logger} from "..";
import {TYPES} from "../setup/Types";
import {ApplicationOptions} from "./ApplicationOptions";
import {ResourceInterface} from "./ResourceInterface";

@provide(Application)
export class Application {
    private expressApplication: express.Application;
    private options: ApplicationOptions;
    private logger: Logger;

    constructor(@inject(TYPES.ExpressApplication) expressApplication: express.Application,
                @inject(TYPES.ApplicationOptions) options: ApplicationOptions,
                logger: Logger) {
        this.expressApplication = expressApplication;
        this.options = options;
        this.logger = logger;

        this.initialize();
    }

    public addResource(name: string, resource: ResourceInterface): void {
        this.expressApplication.use("/" + name, resource.getRoutes());
    }

    public start(): void {
        const port = this.options.port;

        this.expressApplication.listen(port, (error: Error) => {
            if (error) {
                this.logger.error(error);
            } else {
                this.logger.info(`Started server at http://localhost:${port}`);
            }
        });
    }

    private initialize(): void {
        this.expressApplication.use(Compression());
        this.expressApplication.use(Helmet());
        this.expressApplication.use(BodyParser.urlencoded({extended: true}));
        this.expressApplication.use(BodyParser.json());
    }
}