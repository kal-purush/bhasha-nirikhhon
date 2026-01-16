import { Document, Model } from 'mongoose';
import mongoose from 'mongoose';

export interface ContentRatingAttrs {
    userId: string;
    contentId: string;
    contentType: string;
    rating: number;
}

export interface ContentRatingModel extends Model<ContentRatingDocument> {
    addOne(doc: ContentRatingAttrs): ContentRatingDocument;
}

export interface ContentRatingDocument extends Document {
    userId: string;
    contentId: string;
    contentType: string;
    rating: number;
}

const contentRatingSchema = new mongoose.Schema({
    userId: {
        type: String,
        required: true
    },
    contentId: {
        type: String,
        required: true
    },
    contentType: {
        type: String,
        required: true
    },
    rating: {
        type: Number,
        required: true
    },
},
{
    timestamps: true
});

contentRatingSchema.index(
    { userId: 1, contentId: 1, contentType: 1 },
    { unique: true }
);

export default mongoose.model('ContentRating', contentRatingSchema);