import pool from '@/lib/db';
import { NextApiRequest, NextApiResponse } from 'next';

interface dbCat{
    menu_category: string;
  };
export default async (req: NextApiRequest, res: NextApiResponse) => {
    if (req.method === 'GET') {
        //쿼리문으로 내용을 바꾸어야 합니다.
        const { storeId } = req.query;
        console.log("api" ,storeId);
        const [rows] = await pool.execute(
            'SELECT DISTINCT menu_category FROM kiossgk.Menu WHERE store_idx = ? AND menu_category IS NOT NULL',
            [storeId]
          );
                  // 쿼리 결과를 배열로 변환합니다.
        const dbCategories = JSON.parse(JSON.stringify(rows));

        // 맨 위에 '전체 메뉴' 삽입
        dbCategories.unshift({ menu_category: '전체 메뉴' });
          console.log(dbCategories);
          const categories = dbCategories.map((dbCat: { menu_category: string; }, index: number) => ({
            index: index + 1,
            item: dbCat.menu_category
        }));
        
        
        res.status(200).json(categories);
    } else {
        res.status(405).json({ message: 'Method not allowed' });
    }
    };