var mysql = require("mysql2");
require("dotenv").config();
// 연결 설정
const config = {
  host: "localhost",
  port: 3306,
  user: process.env.USER,
  password: process.env.PASSWORD,
  database: 'BookCycle',
  connectTimeout: 20000, // 20초
  waitForConnections: true,
  connectionLimit: 10,
  maxIdle: 10, // max idle connections, the default value is the same as `connectionLimit`
  idleTimeout: 60000, // idle connections timeout, in milliseconds, the default value 60000
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0
};

// 연결 풀 생성
const pool = mysql.createPool(config);

// 프로미스 래퍼 사용
const promisePool = pool.promise();

// 연결 테스트
pool.getConnection((err, connection) => {
  if (err) {
    if (err.code === 'PROTOCOL_CONNECTION_LOST') {
      console.error('Database connection was closed.');
    }
    if (err.code === 'ER_CON_COUNT_ERROR') {
      console.error('Database has too many connections.');
    }
    if (err.code === 'ECONNREFUSED') {
      console.error('Database connection was refused.');
    }
    if (err.code === 'ETIMEDOUT') {
      console.error('Database connection timed out.');
    }
  }
  if (connection) {
    connection.release();
    console.log('MySQL 연결 성공');
  }
  return;
});

module.exports = pool;