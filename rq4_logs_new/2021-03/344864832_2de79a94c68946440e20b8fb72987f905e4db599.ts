import mongoose from 'mongoose';
import GridFsStorage from 'multer-gridfs-storage';
import multer from 'multer';
import crypto from "crypto";
import path from "path";

const connectDb = () => {
    return mongoose.connect(process.env.DATABASE_URL as string
        , { useNewUrlParser: true, useUnifiedTopology: true });
};

// let gfs: any;
// connectDb().then((result) => {
//     if (result && result.connection && result.connection.db ) {
//         gfs = new mongoose.mongo.GridFSBucket(result.connection.db, {
//             bucketName: "uploads"
//           });
//     }
// });

const storage = new GridFsStorage({
    url: process.env.DATABASE_URL as string,
    file: (req, file) => {
      return new Promise((resolve, reject) => {
        crypto.randomBytes(16, (err, buf) => {
          if (err) {
            return reject(err);
          }
          const filename = buf.toString("hex") + path.extname(file.originalname);
          const fileInfo = {
            filename: filename,
            bucketName: "uploads"
          };
          resolve(fileInfo);
        });
      });
    }
  });
  
const upload = multer({
    storage
});

export {
    // gfs,
    upload
};

export default connectDb;