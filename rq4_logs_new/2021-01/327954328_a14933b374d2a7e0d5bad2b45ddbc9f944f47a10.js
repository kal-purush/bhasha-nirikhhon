const mongoose = require("mongoose")
const Schema = mongoose.Schema

const todos = new Schema({
  title: {
    type: String,
    maxlength: 200,
    required: true
  },
  done: {
    type: Boolean,
    require: true, 
    default: false
  }
})

module.exports = mongoose.model("todos", todos)