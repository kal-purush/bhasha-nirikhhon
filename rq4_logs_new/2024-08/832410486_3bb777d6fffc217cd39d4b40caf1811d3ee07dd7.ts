// pages/api/admin_menu_api/store.ts
import { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';
import { RowDataPacket } from 'mysql2';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    const { adminId } = req.query;

    if (req.method !== 'GET') {
        return res.status(405).json({ message: 'Method Not Allowed' });
    }

    try {
        const [rows] = await pool.query<RowDataPacket[]>(
            'SELECT store_idx FROM Store WHERE admin_idx = ?', 
            [adminId]
        );

        if (rows.length > 0) {
            const storeId = rows[0].store_idx;
            return res.status(200).json({ storeId });
        } else {
            return res.status(404).json({ message: 'Store not found' });
        }
    } catch (error) {
        console.error('Error fetching store:', error);
        return res.status(500).json({ message: 'Internal Server Error' });
    }
}