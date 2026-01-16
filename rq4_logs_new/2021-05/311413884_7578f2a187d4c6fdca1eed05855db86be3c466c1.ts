import AWS from "aws-sdk";
import dotenv from "dotenv";

dotenv.config();

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

export const uploadFile = async (buffer: Buffer, key: string) => {
  const params: AWS.S3.PutObjectRequest = {
    Bucket: process.env.AWS_S3_BUCKET!,
    Key: key,
    Body: buffer,
    ACL: "public-read",
  };

  return new Promise((resolve, reject) => {
    s3.upload(params, (err: any, data: any) => {
      if (err) reject(err);
      resolve(data);
    });
  });
};