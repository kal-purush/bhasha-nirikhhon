import { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'POST') {
        return res.status(405).end(); // 허용되지 않는 메소드
    }

    const { email } = req.body;

    if (!email) {
        return res.status(400).json({ error: '잘못된 요청입니다.' });
    }

    try {
        const [rows]: [any[], any] = await pool.query('SELECT COUNT(*) as count FROM Admin WHERE email = ?', [email]);
        
        if (rows[0].count > 0) {
            return res.status(200).json({ message: '이미 존재하는 이메일입니다.' });
        } else {
            return res.status(200).json({ message: '사용 가능한 이메일입니다.' });
        }
    } catch (error) {
        // 타입 단언을 사용하여 error가 Error 객체임을 명시
        const err = error as Error;
        console.error('DB 연결 오류:', err);
        return res.status(500).json({ error: '데이터베이스 오류가 발생했습니다.', details: err.message });
    }
}