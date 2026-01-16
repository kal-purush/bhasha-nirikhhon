const mongoose=require('mongoose');
const {Schema,model}=mongoose;

const bookingListSchema=new Schema({
    name:String,
    location:String,
    meetupType:String,
    date:String,
    shift:String,
    capacity:Number,
    cost:Number,
    overallCost:Number,
    userId:{
        type:Schema.Types.ObjectId,
        ref:'User'
    }
})

const BookingList=model('BookingList',bookingListSchema)
module.exports=BookingList;