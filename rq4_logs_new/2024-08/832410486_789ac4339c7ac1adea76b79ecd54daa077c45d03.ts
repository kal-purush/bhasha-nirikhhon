import { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';
import { RowDataPacket } from 'mysql2';

export default async (req: NextApiRequest, res: NextApiResponse) => {
    if (req.method === 'GET') {  
        try {
        const { menuId } = req.query;
        //console.log("get요청 메뉴 하나 시작");
        //console.log(menuId);
        const [rows] = await pool.query<RowDataPacket[]>(
          `SELECT option_idx, menu_idx, options, price, status
            FROM  kiossgk.MenuOption 
            WHERE menu_idx = ?  AND status = 'available'`, [menuId]);
          // console.log(rows);
        res.status(200).json(rows);
      } catch (error) {
        console.error('메뉴 옵션 조회 중 오류 발생:', error);
        res.status(500).json({ message: 'Internal Server Error' });
      }
    } else {
        res.status(405).json({ message: 'Method not allowed' });
    }
};