import { NextFunction, Request, Response } from 'express';
import upload from '../middleware/multer-s3';
import { IUserMediaService } from '../services/interfaces/IUserMediaService';
import { CustomError } from '../errors/customErrors';
import { HttpStatusCodes } from '../config/HttpStatusCodes';
import { MediaType } from '../models/mediaModel';

export class UserMediaController {
  private userMediaService: IUserMediaService;

  constructor(userMediaService: IUserMediaService) {
    this.userMediaService = userMediaService;
  }

  createPost(req: Request, res: Response, next: NextFunction): void {
    console.log(req.body)
    upload.single('media')(req, res, async (err: any) => {
      if (err) {
        return next(
          new CustomError(
            `File upload failed: ${err.message}`,
            HttpStatusCodes.BAD_REQUEST
          )
        );
      }
      console.log(req.file)

      try {
        const { userId } = req.params;
        const { text, mediaDescription } = req.body;

        if (!userId) {
          throw new CustomError('User ID is required', HttpStatusCodes.BAD_REQUEST);
        }

        const media:any = req.file; // Multer S3 stores the file URL in `file.location`
        const newPost = {
          text: text?.trim() || undefined,
          media: media
            ? {
                type: MediaType.IMAGE, 
                url: media.location,
                description: mediaDescription?.trim() || undefined,
              }
            : undefined,
        };

        const createdPost = await this.userMediaService.createPost(userId, newPost);

        res.status(HttpStatusCodes.CREATED).json({
          message: 'Post created successfully',
          data: createdPost,
        });
      } catch (error: any) {
        console.error('Error creating post:', error.message);
        next(
          new CustomError(
            error.message || 'Internal Server Error',
            error.status || HttpStatusCodes.INTERNAL_SERVER_ERROR
          )
        );
      }
    });
  }
}