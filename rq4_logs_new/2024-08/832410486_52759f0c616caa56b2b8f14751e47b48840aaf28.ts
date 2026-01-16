// src/lib/db.ts
import mysql, { Pool } from 'mysql2/promise';

const pool: Pool = mysql.createPool({
 //여기 부분이 자바에서 설정했던 프로퍼티스 부분이랑 같은 부분 입니당.
  host: 'localhost',
  user: 'root',
  password: '1234',
  database: 'kiosk',
  //여기 부분 부터는 기타 옵션설정인데, 없어도 되나, 추천이 있어서 적용해두었습니다.
  waitForConnections: true, // 만약  db가 연결이 안되면 연결이 되기를 기다리냐, 아니면 바로 에러를 띄우냐를 묻는 옵션입니다. 현재는 기다리게 설정 되어있습니다.
  connectionLimit: 10,  // 한번에 db랑 연결을 몇개까지 할 수 있는지에 대한 설정입니다. 나중에 늘리거나 줄일 수 있습니다.
  queueLimit: 0 //연결을 기다리는 수를 몇개나 둘거냐 라는 옵션인데 0이라고 하면 그 큐가 무한대로 늘어날 수 있음을 의미합니다.
});

export default pool;