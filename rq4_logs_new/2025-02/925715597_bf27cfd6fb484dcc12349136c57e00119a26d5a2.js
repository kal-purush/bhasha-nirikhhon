import mongoose from "mongoose";

const ExtraClassSchema = new mongoose.Schema({
  userId: mongoose.Schema.Types.ObjectId,
  date: { type: String, required: true },
  day: { type: String, required: true },
  subjects: [
    {
      name: { type: String, required: true },
      time: { type: String, required: true },
    },
  ],
});

const ExtraClass = mongoose.model("ExtraClass", ExtraClassSchema);
export default ExtraClass;