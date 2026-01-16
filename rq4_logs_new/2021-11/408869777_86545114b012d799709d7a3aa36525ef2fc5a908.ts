import { MediaIconHandlers } from 'env';
import { prisma } from '../../../../prisma/prismaClient';

const getOneMediaIcon: MediaIconHandlers['getOne'] = async (req, res, next) => {
  const { id } = req.params;
  try {
    const mediaIcon = await prisma.mediaIcon.findUnique({
      where: {
        id,
      },
      select: {
        id: true,
        name: true,
        iconUrl: true,
        mediaLink: true,
      },
    });
    res.status(200).json(mediaIcon);
  } catch (error) {
    next(error);
  }
};

export default getOneMediaIcon;