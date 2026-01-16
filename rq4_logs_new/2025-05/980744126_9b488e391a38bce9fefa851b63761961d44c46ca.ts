import mongoose, { model, Schema } from "mongoose";

const chatSchema = new Schema(
  {
    users: {
      type: [Schema.Types.ObjectId],
      ref: "User",
    },
    createdBy: {
      type: Schema.Types.ObjectId,
      ref: "User",
    },
    latestMessage: {
      type: Schema.Types.ObjectId,
      ref: "Message",
    },
    isGroupChat: {
      type: Boolean,
      default: false,
    },
    groupName: {
      type: String,
      default: "",
    },
    groupProfilePicture: {
      type: String,
      default: "",
    },
    groupBio: {
      type: String,
      default: "",
    },
    groupAdmin: {
      type: [Schema.Types.ObjectId],
      ref: "User",
    },
    unreadCounts: {
      type: Object,
      default: {},
    },
  },
  { timestamps: true, toJSON: { virtuals: true } }
);

if (mongoose.models && mongoose.models["Chat"]) {
  mongoose.deleteModel("Chat");
}

export const Chat = model("Chat", chatSchema);