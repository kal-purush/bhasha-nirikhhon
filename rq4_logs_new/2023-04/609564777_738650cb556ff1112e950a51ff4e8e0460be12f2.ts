import type { NextApiRequest, NextApiResponse } from 'next';
import { getCloudinaryImageUrl, Options } from '@/utils/cloudinary';

type Data = {
  imageUrl: string;
};

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<Data>
) {
  const publicId = req.query.publicId as string;
  const options = req.query.options;

  const imageUrl = getCloudinaryImageUrl(publicId, options);

  res.status(200).json({ imageUrl });
}