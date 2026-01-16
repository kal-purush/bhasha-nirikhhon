import { Request, Response } from "express";
import * as paymentService from "../services/paymentService.js";

export async function payService(req: Request, res: Response) {
    const { cardId } = req.params;
    const { password, businessId, amount } = req.body;

    await paymentService.validatePayment(parseFloat(cardId), password, parseFloat(businessId), amount);
    await paymentService.pay(parseFloat(cardId), parseFloat(businessId), amount);

    res.status(200).send('opa');
}