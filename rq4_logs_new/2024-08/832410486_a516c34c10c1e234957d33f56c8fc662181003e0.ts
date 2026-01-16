import { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';
import { ResultSetHeader, RowDataPacket } from 'mysql2';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { method } = req;

  switch (method) {
    case 'GET':
      return handleGet(req, res);
    case 'PUT':
      return handleUpdate(req, res);
    default:
      res.setHeader('Allow', ['GET', 'POST', 'PUT', 'DELETE']);
      return res.status(405).end(`Method ${method} Not Allowed`);
  }
}

async function handleGet(req: NextApiRequest, res: NextApiResponse) {
  try {
    const { storeId } = req.query;

    const [rows] = await pool.query<RowDataPacket[]>(
      `SELECT o.order_idx, o.user_idx, o.store_idx, o.created, o.order_date, m.menu_name, c.count
       FROM orders o
       LEFT JOIN CartItems c ON o.cart_idx = c.cart_idx
       LEFT JOIN Menu m ON c.menu_idx = m.menu_idx 
       WHERE o.store_idx = ? and o.order_state = '03'`, [storeId]);

    res.status(200).json(rows);
  } catch (error) {
    console.error('주문 조회 중 오류 발생:', error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
}

async function handleUpdate(req: NextApiRequest, res: NextApiResponse) {
  try {
    const { orderId, orderState } = req.body;
    await pool.query('UPDATE orders SET order_state = ? WHERE order_idx = ?', [orderState, orderId]);
    res.status(200).json({ message: '주문 상태가 업데이트 되었습니다' });
  } catch (error) {
    console.error('메뉴 삭제 중 오류 발생:', error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
}