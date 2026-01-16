import next from "next";
import { initMongoose } from "../../../lib/mongoose";
import Replies from "../../../models/Replies";

export async function findAllReplies() {
  return Replies.find().exec();
}

export async function handler(req, res) {
  await initMongoose();
  
  res.status(201).json(await findAllReplies());
}