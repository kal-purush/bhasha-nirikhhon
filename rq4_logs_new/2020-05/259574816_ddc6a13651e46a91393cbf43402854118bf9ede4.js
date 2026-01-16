// Mongoose stuff
mongoose = require('mongoose');

const postSchema = new mongoose.Schema({
  id: Number,
  body: String,
  title: String,
  userId: Number,
  

});

const Post = mongoose.model('Post', postSchema);

module.exports = Post;