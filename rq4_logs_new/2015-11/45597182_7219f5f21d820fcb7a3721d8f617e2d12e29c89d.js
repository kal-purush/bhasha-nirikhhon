var mysql = require('mysql');

var dbConn = mysql.createConnection({
	connectionLimit : 100, //important
	host     : process.env.CB_MYSQL_SERVER,
	user     : process.env.CB_MYSQL_USER,
	password : process.env.CB_MYSQL_PASSWORD,
	port     : process.env.CB_MYSQL_PORT,
	database : process.env.CB_MYSQL_DB_NAME
});


/*Connect to the DB*/
dbConn.connect(function(err) {
  if (err) {
    console.error('error connecting: ' + err.stack);
    return;
  }
 
  console.log('connected as id ' + connection.threadId);
});


exports.connect= function () {
        console.log('db connection started.. ' + dbConn);

	/*
	dbConn.connect(function(err) {
		if (err) {
			console.error('DB: error connecting: ' + err.stack);
			return;
		}
 
		console.log('DB: connected as id ' + connection.threadId);
	}); */
}