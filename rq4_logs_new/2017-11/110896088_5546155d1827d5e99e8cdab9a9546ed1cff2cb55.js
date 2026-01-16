var mongoose = require('mongoose');
var CommentSchema = new mongoose.Schema({
  name: String,
  caption: String,
  link: String,
  upvotes: {type: Number, default: 0},
});
mongoose.model('Pictures', CommentSchema);