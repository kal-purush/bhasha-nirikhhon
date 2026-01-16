import mongoose, { Schema, Document } from 'mongoose';

export interface IHeadline extends Document {
    source: string,
    title: string,
    content: string,
    publishedAt: Date
};

const headlineSchema: Schema = new Schema({
    source: { type: String, required: true, lowercase: true},
    title: { type: String, required: true, lowercase: true},
    content: { type: String, required: true, lowercase: true},
    publishedAt: { type: Date, required: true}
});

export default mongoose.model<IHeadline>('Headline', headlineSchema);