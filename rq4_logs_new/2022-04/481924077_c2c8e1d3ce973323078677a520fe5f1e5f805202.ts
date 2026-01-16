import { Router } from "express";
import { rechargeCard } from "../controllers/rechargeController.js";
import apiKeyValidationMiddleware from "../middlewares/apiKeyValidationMiddleware.js";
import validateSchemaMiddleWare from "../middlewares/validateSchemaMiddleware.js";
import rechargeCardSchema from "../schemas/rechargeCardSchema.js";


const rechargeRouter = Router();
rechargeRouter.post(
    '/recharge/:cardId',
    validateSchemaMiddleWare(rechargeCardSchema),
    apiKeyValidationMiddleware,
    rechargeCard
);

export default rechargeRouter;