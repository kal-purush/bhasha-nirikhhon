import {Request, Response, Router} from "express";
import {AppDataSource} from "@root/data-source";
import PizzaSize from "@root/entity/PizzaSize";

const PizzaSizeRepository = AppDataSource.getRepository(PizzaSize);

interface IPizzaLookupTable {
  [key: string]: {
    price: number;
    name: string;
  };
}

const lk = Router();

lk.get('/pizza', async (req: Request, res: Response) => {
  const result: Array<IPizzaLookupTable> = (await PizzaSizeRepository.find({relations: {pizza: {sizes: true, category: true, extras: true, crusts: true, images: true}}})).map((p: PizzaSize) => {
    const temp: IPizzaLookupTable = {};

    temp[p.pizzaNameID] = {
      name: p.pizza.name,
      price: p.price
    }

    return temp;
  })

  res.Ok(result)
})

module.exports = lk;