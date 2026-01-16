var ibmdb = require('ibm_db');

var myhost = "localhost";
var dbname = "TEST";
var db2user = "db2inst1";
var password = "db2inst1";
var dbport = "50000";


ibmdb.open("DRIVER={DB2};DATABASE="+dbname+";HOSTNAME="+myhost+";UID="+db2user+";PWD="+password+";PORT="+dbport+";PROTOCOL=TCPIP", function (err,conn) {
  if (err) return console.log(err);

  conn.query('select 1 from sysibm.sysdummy1', function (err, data) {
    if (err) console.log(err);
    else console.log(data);

    conn.close(function () {
      console.log('done');
    });
  });
});