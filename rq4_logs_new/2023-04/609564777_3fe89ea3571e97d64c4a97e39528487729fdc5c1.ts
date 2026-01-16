import type { NextApiRequest, NextApiResponse } from 'next';
import { getImageFromS3 } from '@/utils/s3';
import dotenv from "dotenv";
dotenv.config();


export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    const { key } = req.query;
    const bucket = process.env.AWS_S3_BUCKET;

    if (!key || typeof key !== 'string') {
      res.status(400).json({ error: 'Missing "key" parameter' });
      return;
    }

    const image = await getImageFromS3(bucket, key);
    res.setHeader('Content-Type', image.ContentType);
    res.send(image.Body);
  } catch (error) {
    console.error('Error in API route:', error);
    res.status(500).json({ error: 'Error fetching image' });
  }
}