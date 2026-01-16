const mongoose = require('mongoose')

// This is will help MongoDb store the post information by providing the structure or schema
// Define post schem
const postSchema = new mongoose.Schema({
    userId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'User', // Reference to the User model
        required: true,
        index: true, 
    },
    title: {
        type: String,
        required: true
    },
    message: String
});

// Create and export post model
const Post = mongoose.model('Post', postSchema);
module.exports = Post;