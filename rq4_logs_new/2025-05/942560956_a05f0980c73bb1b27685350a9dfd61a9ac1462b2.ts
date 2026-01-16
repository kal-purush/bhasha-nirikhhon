// pages/api/request-password-reset.ts
import type { NextApiRequest, NextApiResponse } from 'next';

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== 'PUT') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { email } = req.body;

    if (!email) {
      return res.status(400).json({ error: 'Email is required' });
    }

    console.log('Sending reset link to:', email);
    // You can implement actual email logic here.

    return res.status(200).json({ message: 'Reset link sent successfully' });
  } catch (error) {
    console.error('Error in API:', error);
    return res.status(500).json({ error: 'Server error' });
  }
}