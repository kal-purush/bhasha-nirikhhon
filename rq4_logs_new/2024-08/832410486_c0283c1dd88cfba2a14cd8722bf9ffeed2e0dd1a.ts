import { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';
import { ResultSetHeader, RowDataPacket } from 'mysql2';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    const { method } = req;

    switch (method) {
    case 'GET':
      return handleGet(req, res);
    default:
      res.setHeader('Allow', ['GET', 'POST', 'PUT', 'DELETE']);
      return res.status(405).end(`Method ${method} Not Allowed`);
  }
    
}

async function handleGet(req: NextApiRequest, res: NextApiResponse) {
  try {
    const { storeId } = req.query;

    const [rows] = await pool.query<RowDataPacket[]>(
      `SELECT a.admin_name 
       FROM Admin a 
       WHERE a.admin_idx = ?`, [storeId]);

    return res.status(200).json(rows);
  } catch (error) {
    console.error('메뉴 조회 중 오류 발생:', error);
    return res.status(500).json({ message: 'Internal Server Error' });
  }
}