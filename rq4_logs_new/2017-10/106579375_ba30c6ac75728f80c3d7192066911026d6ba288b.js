var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var Tweet = new Schema({
  user: {
    type: String
  },
  body: {
    type: String
  },
  imagePath: {
    type: String
  }
},{
    collection: 'tweets'
});

module.exports = mongoose.model('Tweet', Tweet);
