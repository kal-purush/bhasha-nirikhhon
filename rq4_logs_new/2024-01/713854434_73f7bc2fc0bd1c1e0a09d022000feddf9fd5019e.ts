import mongoose from "mongoose";

const { Schema } = mongoose;

const roomSchema = new Schema(
  {
    _id: { type: mongoose.Schema.Types.ObjectId, auto: true },
    members: [{ type: String }],
    messages: [{
        time : {type:Date},
        sender : {type:String},
        text : {type:String}
     }],
    
  },
  { timestamps: true }
);

export default mongoose.models.Room || mongoose.model("Room", roomSchema);