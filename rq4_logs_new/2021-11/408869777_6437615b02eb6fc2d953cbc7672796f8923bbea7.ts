import { PostHandlers } from 'env';
import postComment from 'src/api/comments/controllers/post';
import { prisma } from '../../../../prisma/prismaClient';

const getComments: PostHandlers['getComments'] = async (req, res, next) => {
  const { id } = req.params;
  try {
    const postComments = await prisma.post
      .findUnique({
        where: {
          id,
        },
      })
      .comments();
    res.status(200).json(postComments);
  } catch (err) {
    next(err);
  }
};

export default getComments;