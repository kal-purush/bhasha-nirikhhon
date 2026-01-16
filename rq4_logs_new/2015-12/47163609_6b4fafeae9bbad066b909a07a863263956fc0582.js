var mongoose = require('mongoose');
mongoose.connect('mongodb://localhost/icecream-db');

mongoose.set('debug', true);

module.exports.Icecream = require('./icecream');