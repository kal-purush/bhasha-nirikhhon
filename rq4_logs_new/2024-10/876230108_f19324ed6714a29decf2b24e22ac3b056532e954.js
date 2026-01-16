const mysql = require('mysql2');
require('dotenv').config();

const db = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "arpita123",
  database: process.env.DB_NAME
});

db.connect((err) => {
  if (err) {
    console.error('Database connection failed:', err.stack);
    return;
  }
  console.log('Connected to MySQL');
});

module.exports = db;