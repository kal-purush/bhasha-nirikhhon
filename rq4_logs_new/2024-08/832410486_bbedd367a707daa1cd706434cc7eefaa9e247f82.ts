import { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';
import { ResultSetHeader, RowDataPacket } from 'mysql2';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { method } = req;

  switch (method) {
    case 'GET':
      return handleGet(req, res);
    case 'DELETE':
      return handleDelete(req, res);
    default:
      res.setHeader('Allow', ['GET', 'POST', 'PUT', 'DELETE']);
      return res.status(405).end(`Method ${method} Not Allowed`);
  }
}

async function handleGet(req: NextApiRequest, res: NextApiResponse) {
  try {
    const { storeId } = req.query;

    const [rows] = await pool.query<RowDataPacket[]>(
      `SELECT o.order_idx, o.user_idx, o.store_idx, o.cart_state, o.created, o.order_date
       FROM oders o
       LEFT JOIN Carts ct ON o.cart_idx = ct.cart_idx 
       WHERE m.store_idx = ?`, [storeId]);

    res.status(200).json(rows);
  } catch (error) {
    console.error('메뉴 조회 중 오류 발생:', error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
}

async function handleDelete(req: NextApiRequest, res: NextApiResponse) {
  try {
    const { id } = req.body;
    await pool.query('DELETE FROM Menu WHERE menu_idx = ?', [id]);
    await pool.query('DELETE FROM Menuimg WHERE menu_idx = ?', [id]);
    res.status(200).json({ message: '메뉴가 삭제되었습니다.' });
  } catch (error) {
    console.error('메뉴 삭제 중 오류 발생:', error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
}