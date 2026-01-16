var mysql = require('mysql');
const database = {
    host: process.env.HOST, 
    user: process.env.USER, 
    database: process.env.DATABASE, 
    password: process.env.PASS 
};
const connection = mysql.createConnection(database)

connection.connect((err) => {
    if (err) {
        console.error('error conecting: ' + err.stack);
        return;
    }
    else{
        console.log("Connected to DB")
    }
});

module.exports = connection;