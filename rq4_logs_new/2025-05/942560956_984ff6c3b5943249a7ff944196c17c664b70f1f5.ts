import type { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db'; // תוודא שזה הנתיב הנכון למאגר שלך

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const result = await pool.query('SELECT * FROM medical_equipment ORDER BY id ASC');
    return res.status(200).json(result.rows);
  } catch (error) {
    console.error('Error fetching equipment:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
}