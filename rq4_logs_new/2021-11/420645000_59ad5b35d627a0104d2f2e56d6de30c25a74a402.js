const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const TempUser = new Schema({
    // username: {
    //     type: String,
    //     required: true,
    //     min: 3,
    //     max:255
    // },
    // email: {
    //     type:String,
    //     required: true,
    //     min:3,
    //     max: 255
    // },
    phoneNumber:
    {
      type: String,
      required: true,
      max: 1024,
      min:10  
    },

    petId:
    {
        type:String, 
        required:true,
        max: 1024,
        min:6
    },
    password:{
        type: String,
        required: true,
        max: 6,
        min:6
    },
    date: {
        type:Date,
        default: Date.now
    }
},{ collection : 'tempuser' })

module.exports = mongoose.model("TempUser", TempUser);