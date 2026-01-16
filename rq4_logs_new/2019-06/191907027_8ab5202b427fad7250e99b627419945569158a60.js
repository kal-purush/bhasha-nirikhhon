const mongoose = require('mongoose');
const Schema  = mongoose.Schema;

//create Schema
const ItemSchema = new Schema({
    email : {
        type:String,
        required : true
    },
    password : {
        type : String,
        required : true
    },
    date : {
        type : Date,
        default : Date.now // to get particular date
    }
});

module.exports = Item = mongoose.model('item',ItemSchema);