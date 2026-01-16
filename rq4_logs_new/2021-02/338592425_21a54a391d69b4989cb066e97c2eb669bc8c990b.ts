import mongoose, { Schema, Document } from 'mongoose';
const AutoIncrement = require('mongoose-sequence')(mongoose);

export interface IChargePoint extends Document {
    id?: number;
    name: string;
    status?: any;
    created_at?: Date;
    updated_at?: Date;
    deleted_at?: Date;
}

export enum IStatus {
    READY = 'ready',
    CHARGING = 'charging',
    WAITING = 'waiting',
    ERROR = 'error',
}

const ChargePointSchema = new Schema({
    _id: Number,
    name: { type : String , unique : true, required : true },
    status: {
        type: String,
        enum: IStatus,
        default: IStatus.READY
    },
    deleted_at: Date,
}, { _id: false, timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' } });

ChargePointSchema.plugin(AutoIncrement);

export default mongoose.model<IChargePoint>('ChargePoint', ChargePointSchema);