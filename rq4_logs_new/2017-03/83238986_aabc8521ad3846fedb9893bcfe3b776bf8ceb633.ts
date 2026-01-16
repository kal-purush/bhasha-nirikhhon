import * as express from "express";
import app from "../app";
import IRouterHandler = express.IRouterHandler;
import {ApiController} from "../controllers/api.controller";
import {Subscriber} from "rxjs";
import Application = express.Application;
class ApiRoutes {

    constructor() {

    }

    bootRoutes<T>(app: Application): any {

      let apiRoutes = new ApiController(app);

      return  apiRoutes.ApiOgScraperAction('/api/ogscraper').subscribe(
            item => item
        );
    }

}

export { ApiRoutes };
