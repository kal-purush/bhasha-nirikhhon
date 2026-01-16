var mongoose = require('mongoose');

var CorporateSchema = new mongoose.Schema({
  name: String,
  address: String,
  contact: {
    name: String,
    number: Number,
    email: String
  },
  active_flag: {type:Boolean, default: true}
});

mongoose.model('Corporate', CorporateSchema);
module.exports = mongoose.model('Corporate');