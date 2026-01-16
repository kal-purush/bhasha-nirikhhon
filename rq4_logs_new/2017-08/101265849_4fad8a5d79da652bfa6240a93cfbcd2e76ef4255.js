var mongoose = require('mongoose');

var bookingSchema = new mongoose.Schema({

 id : Number,
 1: String,
 2: String,
 3:String,
 4: String,
 5: String


});
module.exports = mongoose.model("Booking" , bookingSchema);