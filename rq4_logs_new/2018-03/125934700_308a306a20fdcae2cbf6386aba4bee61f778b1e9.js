function connect(){
    var mysql = require('mysql');
    
    var con = mysql.createConnection({
      host: "host",
      user: "user",
      password: "pass",
      database: "db"
    });

    return con;
  }
  function getAllRaridade(callback){
    const db = connect();

      db.query('SELECT * FROM Raridade', (err, rows) => {
        if(err) return callback.call(null, err, null);
        return callback.call(null,null,rows);
      });
}

module.exports = {
  connect, getAllRaridade
}