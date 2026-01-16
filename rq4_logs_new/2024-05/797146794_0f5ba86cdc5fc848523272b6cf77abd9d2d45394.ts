import mongoose from "mongoose";

const connectionString = `mongodb+srv://${process.env.MONGODB_USERNAME}:${process.env.MONGODB_PASSWORD}@linkedinclone.tuw69ga.mongodb.net/?retryWrites=true&w=majority&appName=linkedinClone`;

if (!connectionString) {
  throw new Error(
    "Please define the MONGODB_URI environment variable inside env.local"
  );
}

const connectDB = async () => {
  if (mongoose.connection?.readyState >= 1) {
    console.log("---Already connected to MongoDB---");
    return;
  }

  try {
    console.log("Connecting to MongoDB.......");
    await mongoose.connect(connectionString);
    console.log("-------- Connected to MongoDB ---------");
  } catch (error) {
    console.log("Error connectiong to the MongoDB: ", error);
  }
};

export default connectDB;