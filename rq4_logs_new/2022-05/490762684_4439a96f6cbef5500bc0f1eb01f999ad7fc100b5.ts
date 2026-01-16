import { IBasket, Age } from 'interfaces/basket';
import { Document } from 'mongodb';
import {
    Model, model, Schema, SchemaTypes,
} from 'mongoose';

interface IBasketModel extends IBasket, Document {}

const Basket = new Schema(
    {
        items: {
            type: SchemaTypes.Array,
            required: true,
        },
        zip_code: {
            type: SchemaTypes.String,
            required: true,
        },
        age: {
            type: SchemaTypes.String,
            enum: Age,
            default: Age.twentyFour,
            required: false,
        },
        coupon: {
            type: SchemaTypes.String,
            required: false,
        },
        cost: {
            type: SchemaTypes.Number,
            default: 0.00,
            required: true,
        },
    },
    { timestamps: true },
);

const BasketModel: Model<IBasketModel> = model<
    IBasketModel,
    Model<IBasketModel>
>('Basket', Basket);

export default BasketModel;