
module.exports.createTable = function (table) {
  var createTableSQLString = table.create().ifNotExists().toQuery();
  console.log("create table query: ", createTableSQLString);
  return createTableSQLString;
};

module.exports.insert = function(table, entry) {
  //entry {id: id, name: name, email: email}
  var insertSQLString = table.insert(entry).toQuery();
  // console.log("sql query: ", insertString);
  return insertSQLString;
};

module.exports.select = function(table, param) {
  //param {id: id, name: name, email: email}
  var selectString = table.select(param).toQuery();
  // console.log("sql query: ", selectString);
  return selectSQLString;
};

module.exports.runQuery = function(client, sqlString) {
  //helper function to open and close db connection
  client.connect();
  client.query(sqlString, function(err, result) {
    if(err) {
      return console.error('error running query', err);
    }
    client.end();
  });
};