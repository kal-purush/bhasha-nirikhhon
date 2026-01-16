// pages/api/auth/check.ts
import { verifyToken } from '@/utils/auth'; // פונקציה שמוודאת את ה־JWT
import { NextApiRequest, NextApiResponse } from 'next';

export default function handler(req: NextApiRequest, res: NextApiResponse) {
  const token = req.cookies.token;

  if (!token) {
    return res.status(401).json({ error: 'Unauthorized - no token' });
  }

  try {
    const user = verifyToken(token);
    return res.status(200).json({ user });
  } catch (err) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}