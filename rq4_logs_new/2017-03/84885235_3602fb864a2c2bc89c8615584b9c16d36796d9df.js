var mongoose = require('mongoose');

var todoSchema = mongoose.Schema({
  completed: {
    type: Boolean,
    default: false
  },
  description: {
    type: String,
    required: true,
  }
});

var Todo = mongoose.model('Todo', todoSchema);
module.exports = Todo;