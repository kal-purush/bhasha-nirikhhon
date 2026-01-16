import { Promise } from "bluebird";
import { Request, Response } from "express";
import * as _ from "lodash";
import { Log } from "../Log";
import { Station } from "../models";
import { StationInstance } from "../models/station";

const router: any = require("express-promise-router")();

interface StationRequest extends Request {
   newParams: {
      id: string;
   };
}

function extractParams(req: StationRequest, res: Response): any {
   req.newParams = _.pick(req.params, ["id"]);

   return Promise.resolve("next");
}

router.get("/", extractParams, (req: StationRequest, res: Response): any => {
   return Station.findAll()
      .then((stations: StationInstance[]): any => {
         return res.status(200).send(stations);
      });
});

router.get("/:id", extractParams, (req: StationRequest, res: Response): any => {
   return Station.findById(req.newParams.id)
      .then((station: StationInstance | null): any => {
         return res.status(200).send(station);
      });
});

export default router;