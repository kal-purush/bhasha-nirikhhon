import {Request, Response} from "express";
import {z} from "zod";
import logger from "@root/logger";
import {extractErrorsFromZod} from "@root/utils";
import {AppDataSource} from "@root/data-source";
import Order from "@root/entity/Order";
import Number from "@root/schemas/Number";

const OrderRepository = AppDataSource.getRepository(Order);

const UserIDSchema = z.object({
  id: Number,
})

export default async function getOrderHistoryByUserID(req: Request, res: Response) {
  let userID;
  try {
    userID = UserIDSchema.parse({id: req.params.id}).id;
  } catch (error) {
    logger.warn(error);
    return res.BadRequest(extractErrorsFromZod(error))
  }

  try {
    const orders = await OrderRepository.find({
      where: {
        user: {id: userID}
      },
      relations: {user: true, orderItems: {pizza: true, crust: true, outerCrust: true, extra: true, size: true}}
    })

    return res.Ok(orders);
  } catch (e) {
    res.InternalServerError(e);
  }
}