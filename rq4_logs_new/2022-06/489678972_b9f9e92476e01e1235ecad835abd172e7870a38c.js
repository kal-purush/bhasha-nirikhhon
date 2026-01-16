const mongoose = require('mongoose');
const { Schema } = mongoose;

const postSchema = new Schema ({
    title: String,
    subtitle: String,
    author: String,
    category: String,
    content: String
})
const Posts = mongoose.model('posts', postSchema);
module.exports = {Posts, mongoose}