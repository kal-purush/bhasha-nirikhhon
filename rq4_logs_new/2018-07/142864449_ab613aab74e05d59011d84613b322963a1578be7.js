var mysql = require('mysql');

var con = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "qwerty0000",
  database: "pets"
});

con.connect(function(err) {
  if (err) throw err;
  con.query("SELECT name, owner FROM cats", function (err, result, fields) {
    if (err) throw err;
    console.log(result);
    console.log("dsejfi");
    console.log(fields);
  });
});