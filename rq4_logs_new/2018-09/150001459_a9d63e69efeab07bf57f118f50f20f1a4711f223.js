const mongoose = require('mongoose');
/* map global promise */
mongoose.Promise = global.Promise;

mongoose
  .connect('mongodb://localhost/todos')
  .then(() => console.log('Mongodb connected...'))
  .catch(err => console.log(err));

module.exports = { mongoose };