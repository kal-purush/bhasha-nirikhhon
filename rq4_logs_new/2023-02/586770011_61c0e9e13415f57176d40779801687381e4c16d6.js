import mongoose from 'mongoose';
// import autoIncrement from 'mongoose-auto-increment';

const studentStatusSchema = new mongoose.Schema({

    name: String,
    email:String,
    rollno: String,
    yearofgraduating: String,
    status:String,
    hash:String,
    date:Date,
   
});

const studentStatus = mongoose.model('studentStatus', studentStatusSchema);
export default studentStatus;