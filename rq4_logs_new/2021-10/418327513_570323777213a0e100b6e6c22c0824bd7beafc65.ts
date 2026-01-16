import express from "express";
import bodyParser from "body-parser";
import container from "../../../../../infra/inversify/config/config";
import {BaseController} from "../../../../../core/infra/BaseController";
import {TYPES} from "../../../../../infra/inversify/config/types";

const cardOrderingRouter = express.Router();

cardOrderingRouter.use(bodyParser.json());
cardOrderingRouter.post("/", async (req: express.Request, res: express.Response) => {
    await container.get<BaseController>(TYPES.PlaceOrderController).execute(req, res);
});


cardOrderingRouter.get("/status", async (req: express.Request, res: express.Response) => {
    await container.get<BaseController>(TYPES.GetOrdersController).execute(req, res);
});

export {cardOrderingRouter};