import { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db'; // 데이터베이스 연결 설정
import {Order} from '@/types/order';


// export interface Order {
//   order_idx: number;
//   user_idx: string;
//   store_idx: number;
//   cart_idx: number;
//   order_state: string | null;
//   requests: string | null;
//   amount: number | null;
//   agency_id: string | null;
//   created: Date | null;
//   total_price: number | null;
//   order_date: Date | null;
// }
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method === 'POST') {
    try {
      const {
        order_idx,
        requests,
        amount,
        agency_id,
        total_price,
      } = req.body as Partial<Order>;

      const userId = req.cookies.userId; // 쿠키에서 userId 가져오기

      if (!userId) {
        return res.status(400).json({ message: '유효한 유저 ID가 없습니다.' });
      }

      // userId로 Carts 테이블에서 cart_idx, store_idx 조회
      const [rows]: [any[], any] = await pool.query(
        `SELECT cart_idx, store_idx FROM Carts WHERE user_idx = ?`,
        [userId]
      );

      if (rows.length === 0) {
        return res.status(404).json({ message: '해당 유저의 카트를 찾을 수 없습니다.' });
      }

      const { cart_idx, store_idx } = rows[0]; // 첫 번째 행에서 cart_idx와 store_idx 가져오기

      const currentDate = new Date().toISOString(); // 현재 시간 가져오기




      
      // 데이터베이스에 주문 정보 삽입
      await pool.query(
        `INSERT INTO orders (order_idx, user_idx, store_idx, cart_idx, order_state, requests, amount, agency_id, created, total_price, order_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [order_idx, userId, store_idx, cart_idx, '2', requests, amount, agency_id, currentDate, total_price, currentDate]
      );

      // 쿠키를 삭제하여 사용자 세션 초기화
      res.setHeader('Set-Cookie', 'userId=; Path=/; HttpOnly; Expires=Thu, 01 Jan 1970 00:00:00 GMT');
            
      // 응답 반환
      res.status(201).json({ message: '주문이 성공적으로 저장되었습니다.' });

    } catch (error) {
      console.error('주문 저장 중 오류 발생:', error);
      res.status(500).json({ message: '서버 오류가 발생했습니다.' });
    }
  } else {
    res.status(405).json({ message: 'Method not allowed' });
  }
}