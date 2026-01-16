import { connect } from "mongoose";

export const setupMongoDb = async (url: string) => {
    await connect(url);
  };