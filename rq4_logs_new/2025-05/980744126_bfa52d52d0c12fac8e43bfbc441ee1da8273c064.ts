import mongoose from "mongoose";

const userSchema = new mongoose.Schema(
  {
    clerkUserId: {
      type: String,
      required: true,
      unique: true,
    },
    name: {
      type: String,
      required: true,
    },
    userName: {
      type: String,
      required: true,
      unique: true,
    },
    imageUrl: {
      type: String,
    },
    bio: {
      type: String,
    },
  },
  { timestamps: true }
);

if (mongoose.models && mongoose.models["User"]) {
  mongoose.deleteModel("User");
}

export const User = mongoose.model("User", userSchema);