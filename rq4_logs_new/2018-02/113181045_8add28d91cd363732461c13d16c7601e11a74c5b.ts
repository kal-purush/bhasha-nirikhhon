import { Promise } from "bluebird";
import { Request, Response } from "express";
import * as _ from "lodash";
import { Log } from "../Log";
import { Route } from "../models";
import { RouteInstance } from "../models/route";

const router: any = require("express-promise-router")();

interface RouteRequest extends Request {
   newParams: {
      id: string;
   };
}

function extractParams(req: RouteRequest, res: Response): any {
   req.newParams = _.pick(req.params, ["id"]);

   return Promise.resolve("next");
}

router.get("/", extractParams, (req: RouteRequest, res: Response): any => {
   return Route.findAll()
      .then((routes: RouteInstance[]): any => {
         return res.status(200).send(routes);
      });
});

router.get("/:id", extractParams, (req: RouteRequest, res: Response): any => {
   return Route.findById(req.newParams.id)
      .then((route: RouteInstance | null): any => {
         return res.status(200).send(route);
      });
});

export default router;