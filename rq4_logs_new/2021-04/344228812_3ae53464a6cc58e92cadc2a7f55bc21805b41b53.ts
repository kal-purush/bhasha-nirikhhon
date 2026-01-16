import { RequestHandler } from 'express';
import Gallery from '../../model/gallery';
import { GetCurrentUser } from '../../util/request';

const GetPhotoRequestHandler : RequestHandler = async (req, res, next) => {
  try {
    const allUserImages = await Gallery.GetUserGallery(req.params.userId);
    return res.status(200).json({
      result: allUserImages.map((entry) => ({
        title: entry.title,
        description: entry.description,
        imageUrl: entry.imageUrl,
        alreadyLiked: entry.likedBy.has(GetCurrentUser()),
        likeCount: entry.likedBy.size,
      })),
    });
  } catch (error) {
    return next(error);
  }
};

export default GetPhotoRequestHandler;