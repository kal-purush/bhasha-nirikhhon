import type { NextApiRequest, NextApiResponse } from 'next';

export default function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res
      .setHeader('Allow', ['POST'])
      .status(405)
      .end(`Method ${req.method} Not Allowed`);
  }

  const { widgetId } = req.query;

  return res.status(200).json({ name: widgetId });
}