import { mongoose } from 'mongoose';
import dotenv from 'dotenv';
dotenv.config()
const uri = process.env.DATABASE_URI;

const connectDB = async () => {
  mongoose.connect(uri, { useNewUrlParser: true, useUnifiedTopology: true });
  const db = mongoose.connection;

  db.on('error', console.error.bind(console, 'MongoDB connection error:'));
  db.once('open', () => {
    console.log('Connected to MongoDB');
  });
}


export default connectDB;