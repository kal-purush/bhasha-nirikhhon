import mongoose,{ Schema,model } from "mongoose";

const donationSchema = new Schema({
    
},{timestamps:true});

export const donation =  model("donations",donationSchema);