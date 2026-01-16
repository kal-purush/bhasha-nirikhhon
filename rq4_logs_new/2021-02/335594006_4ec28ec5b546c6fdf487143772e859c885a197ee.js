require('dotenv').config();
const mysql = require('mysql');

let connection;

function getConnection() {
  if (!connection) {
    conneciont = mysql.createConnection({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME,
      port: process.env.DB_PORT,
      timezone: 'Europe/Paris',
    });
  }
  if (connection && connection.state === 'disconnected') {
    connection.connect(function bug(err) {
      if (err) {
        console.error(`error connecting : ${err.stack}`);
        return;
      }
      console.log(`connected as id ${connection.threadId}`);
    });
  }
  return connection;
}

module.exports = getConnection();