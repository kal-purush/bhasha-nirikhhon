import {Request, Response} from "express";
import Number from "@root/schemas/Number";
import {z} from "zod";
import {extractErrorsFromZod} from "@root/utils";
import {AppDataSource} from "@root/data-source";
import PizzaSize from "@root/entity/PizzaSize";
import logger from "@root/logger";

const PizzaSizeRepository = AppDataSource.getRepository(PizzaSize);

const PizzaSizeIDSchema = z.object({
  id: Number,
  price: Number,
  image: z.string(),
})

export default async function updatePizzaSize(req: Request, res: Response) {
  let parsedData;

  try {
    parsedData = PizzaSizeIDSchema.parse(req.body);
  } catch (e) {
    return res.BadRequest(extractErrorsFromZod(e));
  }

  logger.debug(parsedData);

  try {
    const pizzaSize = await PizzaSizeRepository.findOne({
      where: {id: parsedData.id},
      relations: {pizza: true}
    })

    if (!pizzaSize) return res.NotFound([{message: `Pizza Size with id ${parsedData.id} not found.`}])

    logger.debug(pizzaSize);

    Object.assign(pizzaSize, parsedData)

    try {
      await PizzaSizeRepository.save(pizzaSize);
    } catch (e) {
      return res.InternalServerError(e);
    }

    return res.Ok(pizzaSize);
  } catch (e) {
    return res.InternalServerError(e);
  }
}