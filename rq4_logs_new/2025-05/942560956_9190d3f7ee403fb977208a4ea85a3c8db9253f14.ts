import { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { id } = req.query; // ה-ID של המשתמש שמעדכנים
  const { full_name, date_of_birth, phone, email, address, image_url } = req.body; // הנתונים החדשים לעדכון

  if (!id || !full_name || !date_of_birth || !phone || !email || !address) {
    return res.status(400).json({ error: 'Missing fields' });
  }

  try {
    // ביצוע השאילתא לעדכון נתונים
    const query = `
      UPDATE users
      SET full_name = $1, date_of_birth = $2, phone = $3, email = $4, address = $5, image_url = $6
      WHERE id = $7
      RETURNING *;
    `;
    const values = [full_name, date_of_birth, phone, email, address, image_url, id];
    
    const result = await pool.query(query, values);

    if (result.rows.length > 0) {
      res.status(200).json({ message: 'User updated successfully', user: result.rows[0] });
    } else {
      res.status(404).json({ error: 'User not found' });
    }
  } catch (error) {
    console.error('Error updating user:', error);
    res.status(500).json({ error: 'Failed to update user' });
  }
}