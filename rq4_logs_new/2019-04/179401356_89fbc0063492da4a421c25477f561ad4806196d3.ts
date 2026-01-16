import * as mongoose from 'mongoose';

export interface ICar extends mongoose.Document {
    model: String;
    brand: String;
    description?: String;
    ref_price?: String;
    category_id: mongoose.Schema.Types.ObjectId;
    created_at: Date;
    updated_at: Date;
    deleted_at?: Date;
}

const schema: any = new mongoose.Schema({
    model: {
        type: String,
        required: true
    },
    brand: {
        type: String,
        required: true
    },
    description: String,
    ref_price: String,
    category_id: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'Category'
    },
    created_at: {
        type: Date,
        default: Date.now
    },
    updated_at: {
        type: Date,
        default: Date.now
    },
    deleted_at: Date
});

export default mongoose.model<ICar>('Car', schema);