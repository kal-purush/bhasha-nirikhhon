import mongoose, { Schema } from 'mongoose';

const postSchema = new Schema({
    title: {
        type: String,
        required: true,
    },
    text: {
        type: String,
        required: true,
    },
    image: {
        data: Buffer,
        contentType: String
    },
    authorId: { type: Schema.Types.ObjectId, ref: 'User' },
    commentIds: [{ type: Schema.Types.ObjectId, ref: 'Comment' }],
    labelIds: [{ type: Schema.Types.ObjectId, ref: 'Label' }],
    // like: [{ type: Schema.Types.ObjectId, ref: 'Like' }]
},
    { timestamps: true });

const Post = mongoose.model('Post', postSchema);

export {
    Post
}