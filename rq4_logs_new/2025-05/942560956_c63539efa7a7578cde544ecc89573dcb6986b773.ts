// pages/api/patients/index.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method === 'GET') {
    try {
      const result = await pool.query('SELECT * FROM patients');
      res.status(200).json(result.rows);
    } catch (error) {
      console.error('Error fetching patients:', error);
      res.status(500).json({ error: 'Database error' });
    }
  } else {
    res.status(405).json({ error: 'Method Not Allowed' });
  }
}