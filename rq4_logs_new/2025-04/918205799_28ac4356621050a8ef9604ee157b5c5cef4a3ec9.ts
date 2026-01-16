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
})

export default async function deletePizzaSize(req: Request, res: Response) {
	let pizzaSizeID;

	try {
		pizzaSizeID = PizzaSizeIDSchema.parse({id: req.params.id});
	} catch (e) {
		return res.BadRequest(extractErrorsFromZod(e));
	}

	logger.debug(pizzaSizeID);

	try {
		const pizzaSize = await PizzaSizeRepository.findOne({
			where: {id: pizzaSizeID.id},
			relations: {pizza: true}
		})

		if (!pizzaSize) return res.NotFound([{message: `Pizza Size with id ${pizzaSizeID.id} not found.`}])

		logger.debug(pizzaSize);


		try {
			await PizzaSizeRepository.delete(pizzaSizeID.id);
		} catch (e) {
			return res.InternalServerError(e);
		}

		return res.Ok(pizzaSize);
	} catch (e) {
		return res.InternalServerError(e);
	}
}