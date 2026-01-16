const mongoose=require("mongoose")

const categorySchema=new mongoose.Schema(
    {
        name:{
            type:String,
            trim:true,
            required:true,
            maxlength:32,
            unique:true
        }
    },
    {timestamps:true} //when we add any new cetegoty the time and date will be save in our datbase 
)