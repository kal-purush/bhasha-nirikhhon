require("dotenv").config();

const mysql = require("mysql2/promise");

const db = mysql.createPool({
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "",
  database: process.env.DB_NAME || "absensi_sekolah",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Test koneksi database
db.getConnection()
  .then((connection) => {
    console.log("✅ Koneksi database berhasil");
    // Test query
    return connection.query("SELECT 1");
  })
  .then(() => {
    console.log("✅ Query test berhasil");
  })
  .catch((err) => {
    console.error("❌ Gagal terhubung ke database:", err);
  });

module.exports = db;