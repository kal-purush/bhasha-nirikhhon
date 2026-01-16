import {Request, Response} from "express";
import {z} from "zod";
import {extractErrorsFromZod} from "@root/utils";
import logger from "@root/logger";
import {AppDataSource} from "@root/data-source";
import OrderItem from "@root/entity/OrderItem";
import Pizza from "@root/entity/Pizza";
import Order from "@root/entity/Order";
import PizzaCrust from "@root/entity/PizzaCrust";
import PizzaExtras from "@root/entity/PizzaExtras";
import PizzaSize from "@root/entity/PizzaSize";
import PizzaOuterCrust from "@root/entity/PizzaOuterCrust";
import NUMBER from "@root/schemas/Number";

const orderItemRepository = AppDataSource.getRepository(OrderItem);
const PizzaRepository = AppDataSource.getRepository(Pizza);
const PizzaCrustRepository = AppDataSource.getRepository(PizzaCrust);
const PizzaExtraRepository = AppDataSource.getRepository(PizzaExtras);
const PizzaSizeRepository = AppDataSource.getRepository(PizzaSize);
const PizzaOuterCrustRepository = AppDataSource.getRepository(PizzaOuterCrust);
const orderRepository = AppDataSource.getRepository(Order);

const CreateCartItemSchema = z.object({
  pizzaID: NUMBER,
  pizzaCrustID: NUMBER.optional(),
  pizzaOuterCrustID: NUMBER.optional(),
  pizzaExtraID: NUMBER.optional(),
  pizzaSizeID: NUMBER.optional(),
  cartID: NUMBER,
  quantity: NUMBER,
  note: z.string().optional(),
})

export default async function addItemToOrder(req: Request, res: Response) {
  let parsed;
  try {
    parsed = CreateCartItemSchema.parse(req.body);
  } catch (error) {
    logger.warn(error);
    return res.BadRequest(extractErrorsFromZod(error));
  }

  try {
    const cartItem = parsed;

    const existedPizza = await PizzaRepository.findOne({
      where: {id: cartItem.pizzaID},
    })

    if (!existedPizza) {
      return res.NotFound([{message: `Pizza with id ${cartItem.pizzaID} not found.`}])
    }

    const existedPizzaExtra = await PizzaExtraRepository.findOne({
      where: {id: cartItem.pizzaExtraID}
    })
    const pizzaExtra = existedPizzaExtra || null;

    const existedPizzaCrust = await PizzaCrustRepository.findOne({
      where: {id: cartItem.pizzaCrustID}
    })
    const pizzaCrust = existedPizzaCrust || null;

    const existedPizzaOuterCrust = await PizzaOuterCrustRepository.findOne({
      where: {id: cartItem.pizzaOuterCrustID}
    })
    const pizzaOuterCrust = existedPizzaOuterCrust || null;

    const existedPizzaSize = await PizzaSizeRepository.findOne({
      where: {id: cartItem.pizzaSizeID}
    })
    const pizzaSize = existedPizzaSize || null;

    const existedCart = await orderRepository.findOne({
      where: {id: cartItem.cartID},
      relations: {
        orderItems: true,
        user: true
      }
    })

    if (!existedCart) {
      return res.NotFound([{message: `Cart with id ${cartItem.cartID} not found.`}])
    }

    if (existedCart.user.id != req.userID) {
      return res.Forbidden([{message: `You cannot access others cart`}])
    }

    const existedCartItem = await orderItemRepository.findOne({
      where: {
        pizza: {id: cartItem.pizzaID},
        crust: {id: cartItem.pizzaCrustID},
        outerCrust: {id: cartItem.pizzaOuterCrustID},
        extra: {id: cartItem.pizzaExtraID},
        size: {id: cartItem.pizzaSizeID},
        order: {id: cartItem.cartID}
      },
      relations: {
        pizza: true, crust: true, outerCrust: true, extra: true, size: true, order: {orderItems: true}
      }
    });

    if (existedCartItem) {
      existedCartItem.quantity += 1;
      await orderItemRepository.save(existedCartItem);

      return res.Ok(existedCartItem);
    }

    const createdCartItem = new OrderItem();
    createdCartItem.quantity = 1;
    createdCartItem.order = existedCart;
    createdCartItem.pizza = existedPizza;
    createdCartItem.crust = pizzaCrust;
    createdCartItem.outerCrust = pizzaOuterCrust;
    createdCartItem.extra = pizzaExtra;
    createdCartItem.size = pizzaSize;
    createdCartItem.note = cartItem.note || "";

    await orderItemRepository.save(createdCartItem);

    res.Ok(createdCartItem);
  } catch (e) {
    logger.error(e);
    res.InternalServerError({});
  }
}