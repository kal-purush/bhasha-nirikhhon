import express, {Request, Response} from "express";
import bodyParser from "body-parser";
import {BaseController} from "../../../../../core/infra/BaseController";
import {TYPES} from "../../../../../infra/inversify/config/types";
import container from "../../../../../infra/inversify/config/config";

const cardRouter = express.Router()

cardRouter.use(bodyParser.json())
cardRouter.post("/", async (req: Request, res:Response) => {
    await container.get<BaseController>(TYPES.CreateCardController).execute(req, res);
})

export {cardRouter};