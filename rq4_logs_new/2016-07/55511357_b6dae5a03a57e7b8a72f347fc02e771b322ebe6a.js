/**
 * Created by bartosz on 26/07/16.
 */
var mongoose = require('mongoose');
mongoose.connect('mongodb://localhost/okrs');

module.exports = mongoose;