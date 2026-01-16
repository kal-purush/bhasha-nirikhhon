import { NextFunction, Request, Response } from 'express';
import memoryUpload from '../utils/memoryUpload';
import APIError from '../utils/APIError';

const uploadToMemory = async(req: Request, res: Response, next: NextFunction): Promise<void> => {
  memoryUpload.single('image')(req, res, (err) => {
    if (err){
      if (!req.file) {
        if (err.message && err.message === 'File too large'){
          return next(new APIError('Error uploading image, The size limit is 2MB', 400));
        }
      }
      if (!req.file) {
        return next(new APIError('no file provided', 400));
      }
      return next(err);
    }
    next();
  });
};

export default uploadToMemory;