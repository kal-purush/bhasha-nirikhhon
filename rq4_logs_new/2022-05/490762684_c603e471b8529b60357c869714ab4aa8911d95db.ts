import { IProduct, Sizes } from 'interfaces/product';
import { Document } from 'mongodb';
import {
    Model, model, Schema, SchemaTypes,
} from 'mongoose';

interface IProductModel extends IProduct, Document {}

const Product = new Schema(
    {
        sku: {
            type: SchemaTypes.String,
            required: true,
        },
        name: {
            type: SchemaTypes.String,
            required: true,
        },
        size: {
            type: SchemaTypes.String,
            enum: Sizes,
            default: Sizes.S,
            required: true,
        },
        price: {
            type: SchemaTypes.Number,
            default: 0.00,
            required: true,
        },
        stockLevel: {
            type: SchemaTypes.Number,
            default: 0,
            required: true,
        },
    },
    { timestamps: true },
);

const ProductModel: Model<IProductModel> = model<
    IProductModel,
    Model<IProductModel>
>('Product', Product);

export default ProductModel;