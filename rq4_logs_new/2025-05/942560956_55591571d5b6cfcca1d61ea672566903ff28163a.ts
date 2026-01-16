import type { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const {
    id,
    full_name,
    date_of_birth,
    phone,
    email,
    address,
    password,
    image_url,
  } = req.body;

  if (!id || !full_name || !date_of_birth || !phone || !email || !address || !password) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    const query = `
      INSERT INTO users (id, full_name, date_of_birth, phone, email, address, password, image_url)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id, full_name, email
    `;
    const values = [id, full_name, date_of_birth, phone, email, address, password, image_url];

    const result = await pool.query(query, values);

    return res.status(201).json({ user: result.rows[0] });
  } catch (error: any) {
    console.error('DB insert error:', error);
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Email or ID already exists' });
    }
    return res.status(500).json({ error: 'Failed to create user' });
  }
}