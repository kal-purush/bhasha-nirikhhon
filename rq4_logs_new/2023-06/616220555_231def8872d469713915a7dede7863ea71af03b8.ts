import { Document, Model } from 'mongoose';
import mongoose from 'mongoose';

export interface ContentCommentAttrs {
    comment: string;
}

export interface ContentCommentModel extends Model<ContentCommentDocument> {
    addOne(doc: ContentCommentAttrs): ContentCommentDocument;
}

export interface ContentCommentDocument extends Document {
    userId: string;
    contentId: string;
    seasonNumber: string;
    episodeNumber: string;
    contentType: string;
    comment: string;
}

const contentCommentSchema = new mongoose.Schema({
    userId: {
        type: String,
        required: true
    },
    contentId: {
        type: String,
        required: true
    },
    seasonNumber: {
        type: String,
        required: true
    },
    episodeNumber: {
        type: String,
        required: true
    },
    contentType: {
        type: String,
        required: true
    },
    comment: {
        type: String,
        required: true
    }
},
{
    timestamps: true
});

export default mongoose.model('ContentComment', contentCommentSchema);