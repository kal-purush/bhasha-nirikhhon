import mongoose from "mongoose";

const Schema = mongoose.Schema;

const userSchema = new Schema ({
    fname : {
        type : String,
        require : true
    },
    lname : {
        type : String,
        require : true
    },
    email : {
        type : String,
        require : type,
        unique : true
    },
    gender : {
        type : String,
        require : true
    },
    techstack : {
        type : String,
        require : true
    },
    interests : {
        type : array,
        require : true
    },
    dateofbirth : {
        type : Date
    },
    profileurl : {
        type : String,
        required : true
    },
    bio : {
        type : String,
        required : true,
        maxlength : 250
    }
})