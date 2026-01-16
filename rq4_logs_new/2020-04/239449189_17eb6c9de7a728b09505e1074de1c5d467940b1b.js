const mysql = require('mysql');

const conn = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "",
  database: "crud-node"
});

conn.connect((err) => {
    if(err) throw err;
    console.log('db running')
});

module.exports = conn;