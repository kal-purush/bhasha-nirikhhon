import type { NextApiRequest, NextApiResponse } from 'next';

export default function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).send({ message: 'Only POST requests are allowed' });
  }
  const body = req.body;
  if (body === process.env.USER_EMAIL) {
    return res.status(200).json(true);
  } else {
    return res.status(400).json(false);
  }
}