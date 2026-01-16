import { RequestHandler } from 'express';
import AWS from 'aws-sdk';
import { GetENVOrThrow } from '../../../util/setup';

const S3_BUCKET_NAME = GetENVOrThrow('GALLERY_BUCKET_NAME');

const AddToMyGalleryRequestHandler : RequestHandler = async (req, res, next) => {
  try {
    console.log(S3_BUCKET_NAME);
    const S3 = new AWS.S3();
    const result = await S3.upload({
      Body: 'Hello World',
      Key: `${Date.now()}`,
      Bucket: S3_BUCKET_NAME,
    }).promise();
    console.log(result);
    console.log(req.body);
    return res.status(200).json({
      message: 'Hello World',
      result,
    });
  } catch (error) {
    return next(error);
  }
};

export default AddToMyGalleryRequestHandler;