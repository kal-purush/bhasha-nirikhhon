// pages/api/sales-data.ts

import type { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db'; // 데이터베이스 연결 설정을 위한 모듈

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    try {
        // SQL 쿼리: 일별 매출 합계 조회
        const [rows]: any = await pool.query(`
            SELECT 
                DATE(order_date) AS order_day,
                SUM(CAST(amount AS DECIMAL(10, 2))) AS daily_total_amount
            FROM 
                orders
            GROUP BY 
                DATE(order_date)
            ORDER BY 
                order_day DESC
        `);

        // 결과를 JSON 형식으로 변환
        const salesData = rows.reduce((acc: any, row: any) => {
            const formattedDate = new Date(row.order_day).toISOString().split('T')[0]; // 날짜를 'YYYY-MM-DD' 형식으로 변환
            const formattedAmount = Math.floor(parseFloat(row.daily_total_amount)); // 소수점 제거

            acc[formattedDate] = formattedAmount;
            return acc;
        }, {});

        // JSON 응답 반환
        res.status(200).json({ dailySales: salesData });
    } catch (error) {
        console.error('Error fetching sales data:', error);
        res.status(500).json({ message: 'Internal Server Error' });
    }
}
