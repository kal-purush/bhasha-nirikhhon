import { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method === 'GET') {
        try {
            const query = `SELECT workplace_idx, workplace_name FROM workplace`;
            const [rows] = await pool.query(query);
            res.status(200).json(rows);
        } catch (error) {
            console.error('DB 연결 오류:', error);
            res.status(500).json({ message: '서버 오류가 발생했습니다.' });
        }
    } else {
        res.status(405).json({ message: '허용되지 않는 메소드입니다.' });
    }
}