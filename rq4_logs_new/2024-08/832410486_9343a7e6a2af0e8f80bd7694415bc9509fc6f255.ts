import { NextApiRequest, NextApiResponse } from 'next';
import pool from '@/lib/db';
import { ResultSetHeader, RowDataPacket } from 'mysql2';
import { Menu } from '@/types/menu'; // Menu 및 Menuimg 타입을 임포트합니다.
import { MenuOption } from '@/types/menuOption';
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { method } = req;

  switch (method) {
    case 'GET':
      return handleGet(req, res);
    case 'POST':
      return handlePost(req, res);
    case 'PUT':
      return handlePut(req, res);
    case 'DELETE':
      return handleDelete(req, res);
    default:
      res.setHeader('Allow', ['GET', 'POST', 'PUT', 'DELETE']);
      return res.status(405).end(`Method ${method} Not Allowed`);
  }
}

async function handleGet(req: NextApiRequest, res: NextApiResponse) {
//   try {
//     const { storeId } = req.query;

//     const [rows] = await pool.query<RowDataPacket[]>(
//       `SELECT m.menu_idx, m.store_idx, m.menu_name, m.menu_price, m.menu_detail, m.menu_category, m.menu_status, mi.menu_image_path 
//        FROM Menu m 
//        LEFT JOIN Menuimg mi ON m.menu_idx = mi.menu_idx 
//        WHERE m.store_idx = ?`, [storeId]);

//     res.status(200).json(rows);
//   } catch (error) {
//     console.error('메뉴 조회 중 오류 발생:', error);
//     res.status(500).json({ message: 'Internal Server Error' });
//   }
}


async function handlePost(req: NextApiRequest, res: NextApiResponse) {
    console.log("옵션 포스트 실행되네용.");
    const { menuId } = req.query; 
    console.log("옵션 포스트 실행되네용.",menuId);
    const { option } = req.body;
    console.log("옵션 포스트 실행되네용.",option);
    const { option_idx, menu_idx, options, price, status } = option;

    try {
      console.log('handlePost 시작');
  
      // 요청 바디에서 데이터 추출
      // const { option_idx, menu_idx, options, price, status } = req.body as MenuOption;
      // console.log("옵션 포스트 실행되네용.",option_idx, menu_idx, options, price, status);
      // 필수 필드 확인
      if (!menuId || !option.options || option.price === undefined || !option.status) {
        console.error('필수 필드가 누락되었습니다:', { menuId, options, price, status });
        return res.status(400).json({ message: '필수 필드가 누락되었습니다.' });
      }
  
      console.log('입력 데이터:', { option_idx, menuId, options, price, status });
  
      // 메뉴 옵션 정보 삽입
      const [result] = await pool.query('INSERT INTO MenuOption (menu_idx, options, price, status) VALUES (?, ?, ?, ?)', [
        menuId,
        options,
        price,
        status,
      ]);
  
      const insertedId = (result as any).insertId; // 삽입된 옵션의 ID를 가져옴
      console.log('옵션 삽입 완료, option_idx:', insertedId);
  
      res.status(201).json({ id: insertedId });
      console.log('handlePost 완료');
    } catch (error) {
      console.error('옵션 생성 중 오류 발생:', error);
      res.status(500).json({ message: 'Internal Server Error' });
    }
//   try {
//     console.log('handlePost 시작'); // 시작점 로그  

//     const { menu_name, menu_price, menu_detail, menu_category, menu_status, menu_image_path } = req.body as Menu & { menu_image_path: string };
//     const { adminId } = req.query; //adminId를 쿼리에서 가져옴

//     // 필수 필드 확인
//     if (!menu_name || !menu_price || !menu_category || !menu_status) {
//       console.error('필수 필드가 누락되었습니다:', { menu_name, menu_price, menu_category, menu_status });
//       return res.status(400).json({ message: '필수 필드가 누락되었습니다.' });
//     }

//     if (!adminId) {
//       console.error('adminId가 없음');
//       return res.status(400).json({ message: 'adminId is required' });
//     }

//     console.log('입력 데이터:', { menu_name, menu_price, menu_detail, menu_category, menu_status, menu_image_path, adminId });

//     // 가게 정보 조회
//     const [storeRow] = await pool.query<RowDataPacket[]>('SELECT store_idx FROM Store WHERE admin_idx = ?', [adminId]);
//     console.log('가게 정보 조회 완료:', storeRow);

//     if (storeRow.length === 0) {
//       console.error('가게를 찾을 수 없음: adminId:', adminId);
//       return res.status(404).json({ message: 'Store not found' });
//     }

//     const store_idx = storeRow[0].store_idx;
//     console.log('store_idx:', store_idx);

//     // 메뉴 정보 삽입
//     const [result] = await pool.query<ResultSetHeader>(
//       'INSERT INTO Menu (store_idx, menu_name, menu_price, menu_detail, menu_category, menu_status) VALUES (?, ?, ?, ?, ?, ?)',
//       [store_idx, menu_name, menu_price, menu_detail, menu_category, menu_status]
//     );

//     const menu_idx = result.insertId;
//     console.log('메뉴 삽입 완료, menu_idx:', menu_idx);

//     if (menu_image_path) {
//       console.log('메뉴 이미지 경로 삽입 시작');
//       await pool.query<ResultSetHeader>(
//         'INSERT INTO Menuimg (menu_idx, menu_image_path) VALUES (?, ?)',
//         [menu_idx, menu_image_path]
//       );
//       console.log('메뉴 이미지 경로 삽입 완료');
//     }

//     res.status(201).json({ id: menu_idx });
//     console.log('handlePost 완료');
//   } catch (error) {
//     console.error('메뉴 생성 중 오류 발생:', error);
//     res.status(500).json({ message: 'Internal Server Error' });
//   }
}

async function handlePut(req: NextApiRequest, res: NextApiResponse) {
  console.log('handlePut 시작'); // 시작 로그

//   try {
//     const { menu_idx, menu_name, menu_price, menu_detail, menu_category, menu_status, menu_image_path } = req.body as Menu & { menu_image_path: string };
   
//     console.log('받은 데이터:', { menu_idx, menu_name, menu_price, menu_detail, menu_category, menu_status, menu_image_path });

//     // menu_idx가 유효한지 확인
//     if (!menu_idx) {
//       console.error('menu_idx가 제공되지 않음');
//       return res.status(400).json({ message: '업데이트 하려면 menu_idx가 필요합니다.' });
//     }

//     // 데이터베이스에서 menu_idx가 존재하는지 확인
//     const [rows] = await pool.query<RowDataPacket[]>('SELECT * FROM Menu WHERE menu_idx = ?', [menu_idx]);
//     console.log('데이터베이스 조회 결과:', rows);

//     if (rows.length === 0) {
//       console.error('해당 메뉴를 찾을 수 없음:', menu_idx);
//       return res.status(404).json({ message: '해당 메뉴를 찾을 수 없습니다.' });
//     }

//     // 메뉴 업데이트 로직
//     const updateResult = await pool.query(
//       'UPDATE Menu SET menu_name = ?, menu_price = ?, menu_detail = ?, menu_category = ?, menu_status = ? WHERE menu_idx = ?',
//       [menu_name, menu_price, menu_detail, menu_category, menu_status, menu_idx]
//     );
//     console.log('메뉴 업데이트 결과:', updateResult);

//     if (menu_image_path) {
//       const imageUpdateResult = await pool.query(
//         'REPLACE INTO Menuimg (menu_idx, menu_image_path) VALUES (?, ?)',
//         [menu_idx, menu_image_path]
//       );
//       console.log('이미지 업데이트 결과:', imageUpdateResult);
//     }

//     res.status(200).json({ message: '메뉴가 수정되었습니다.' });
//     console.log('handlePut 완료'); // 완료 로그
//   } catch (error) {
//     console.error('메뉴 수정 중 오류 발생:', error);
//     res.status(500).json({ message: 'Internal Server Error' });
//   }
}


async function handleDelete(req: NextApiRequest, res: NextApiResponse) {
//   try {
//     const { id } = req.body;
//     await pool.query('DELETE FROM Menu WHERE menu_idx = ?', [id]);
//     await pool.query('DELETE FROM Menuimg WHERE menu_idx = ?', [id]);
//     res.status(200).json({ message: '메뉴가 삭제되었습니다.' });
//   } catch (error) {
//     console.error('메뉴 삭제 중 오류 발생:', error);
//     res.status(500).json({ message: 'Internal Server Error' });
//   }
}