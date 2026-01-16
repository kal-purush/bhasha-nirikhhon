import { Schema, model, connect } from "mongoose";
import Post from "@project1-chat-app/shared";

const PostSchema = new Schema({
  author: String,
  text: String,
  timeStamp: Date,
});

const PostModel = model<Post>("Post", PostSchema);

export const setupMongoDb = async (url: string) => {
  await connect(url);
};

export const loadAllPosts = async (): Promise<Post[]> => {
  return PostModel.find({}).exec();
};

export const savePost = async (post: Post): Promise<void> => {
  const newModel = new PostModel(post);
  newModel.save();
};