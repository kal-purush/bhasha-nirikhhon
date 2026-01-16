var mysql = require('mysql');

var con = mysql.createConnection({
  host: "mammalwebdb.ccrk30vfkswu.eu-west-2.rds.amazonaws.com",
  user: "admin",
  password: "MammalWebAdminDB",
  database: "mammalWeb"
});

function pull_data(sql_query) { 
  con.connect(function(err) {
      if (err) throw err;
      con.query(sql_query, function (err, result, fields) {
        if (err) throw err;
        const fs = require('fs');
        data = JSON.stringify(result, null, 1);
        //data = JSON.parse(data);
        fs.writeFile('data.json', data, (err) => {
          if (err) {
              throw err;
          }
          console.log("JSON data is saved.");
        });
      });
      
  });

};


sql_query = 'SELECT * FROM mammalWeb.Options WHERE struc = "mammal"';
pull_data(sql_query);



