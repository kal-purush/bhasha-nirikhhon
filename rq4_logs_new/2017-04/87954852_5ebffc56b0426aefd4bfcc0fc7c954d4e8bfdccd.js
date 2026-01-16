var mysql = require('mysql');

/*
 * Setup connection to database
 */

module.exports = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'root',
  	socketPath: '/Applications/MAMP/tmp/mysql/mysql.sock',
    //port : 8889, //port mysql
    database: 'kanban'
});
