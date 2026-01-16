import { IRouteSource } from 'interfaces/route';
import BasketModel, { IModelBasket } from 'app/models/Basket';
import {
    OK, NOT_FOUND, CONFLICT, NO_CONTENT, CREATED, ACCEPTED,
} from 'http-status';
import Joi from 'joi';
import Handler from 'core/handler';
import { verifyFields } from 'utils/validate';
import ProductModel from 'app/models/Product';
import { Age } from 'interfaces/basket';

interface IBasketController {
    create: ({ req, res, next }: IRouteSource) => Promise<IModelBasket>;
}

class BasketController implements IBasketController {
    /**
     * @param IRouteSource
     * @returns Promise<any>
     */
    async create({
        req, res, next,
    }: IRouteSource): Promise<any> {
        try {
            verifyFields(
                req.body,
                Joi.object({
                    items: Joi.array().items(Joi.object({
                        sku: Joi.string().regex(/^[a-zA-Z0-9-_]+$/).required(),
                        quantity: Joi.number().required(),
                    }).required()).required(),
                    delivery: Joi.object({
                        zip_code: Joi.string().regex(/^[a-zA-Z0-9-]+$/).required(),
                    }).required(),
                    coupon: Joi.string(),
                }),
            );

            const {
                items, delivery, coupon,
            } = req.body;

            // To-Do - Basket removing by age, maybe use Redis for scheduling Jobs.
            // To-Do - Discount coupons, already being passed on api
            // but will be good to implement at DB level too and discount at the price.

            const skus: string[] = items.map((item: { sku: string, quantity: number }) => item.sku);
            let productPrices: number = 0;

            // eslint-disable-next-line no-plusplus
            for (let i = 0; i < skus.length; i++) {
                const product = await ProductModel.findOne({ sku: skus[i] }).select('-__v');

                if (!product) {
                    throw new Handler(
                        `product with SKU:${skus[i]} not exists.`,
                        CONFLICT,
                    );
                }

                items[i].name = product.name;
                items[i].price = product.price;
                items[i].size = product.size;
                items[i].stockLevel = product.stockLevel;
                productPrices += product.price;
            }

            await BasketModel.create({
                items: items.map((item) => ({ sku: item.sku, quantity: item.quantity })), zip_code: delivery.zip_code, age: Age.twentyFour, coupon, cost: productPrices,
            });

            return res.status(CREATED).json({
                items, delivery, coupon, cost: productPrices,
            });
        } catch (error) {
            return next(error);
        }
    }
}

export default BasketController;