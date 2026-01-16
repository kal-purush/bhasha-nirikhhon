const sql = require("mssql/msnodesqlv8");
const path = require('path');
const configAnh = {
  server: "localhost",
  user: "sa",
  password: "123456",
  server: "DESKTOP-OT7KKUK",
  database: "dialy",
  driver: "msnodesqlv8",

};
const configAn = {
  server: "localhost",
  user: "sa",
  password: "123456",
  server: "DESKTOP-I7P0VBM\\SQLEXPRESS",
  database: "dialy",
  driver: "msnodesqlv8",
  options: {
    trustedconnection: true,
    enableArithAbort: true,
    instancename: "",
  },

};

const configThanh = {
  server: "localhost",
  user: "sa",
  password: "123456",

  server: "DESKTOP-FLTD05G",
  database: "dialy",
  driver: "msnodesqlv8",
  options: {
    trustedconnection: true,
    enableArithAbort: true,
    instancename: "",
  },

};
/*==========================================
---------- CHỈNH CONFIG TẠI ĐÂY ----------
==========================================*/
const config = configAnh;

function getdataHoDuongTinh(res) {


  sql.connect(config, function (err) {

    if (err) {
      console.log(err);
    };

    var request = new sql.Request();

    // query to the database and get the records
    request.query('select * from HoDuongTinh', function (err, recordset) {

      if (err) console.log(err)
      // send records as a response

      return res.send(recordset);

    });
  });


}

function tinhKC(res, x, y) {

  sql.connect(config, function (err) {
    if (err) {
      console.log(err);
    };
    var request = new sql.Request();

    // query to the database and get the records
    request.query(` DECLARE @g geography; 
                     SET @g = geography::Point(${x},${y}, 4326);
                     SELECT TOP (1) H.ID,H.x,H.y,@g.STDistance(H.Toa_do) as KC
                      FROM [HoDuongTinh] H
                      ORDER BY KC
    `
      , function (err, recordset) {

        if (err) console.log(err)


        var json_tao = {
          "ID": recordset.recordset[0].ID,
          "x": recordset.recordset[0].x,
          "y": recordset.recordset[0].y,
          "KC": recordset.recordset[0].KC
        };
        // send records as a response
        res.setHeader('Content-Type', 'application/json');

        return res.json(JSON.stringify(json_tao));



      });
  });


}
function getdataPhongToa(res) {

  sql.connect(config, function (err) {

    if (err) {
      console.log(err);
    };

    var request = new sql.Request();

    // query to the database and get the records
    request.query(`select *, maintoa.Poly.STArea() DienTich
    from VungPhongToa maintoa LEFT JOIN(select pt.ID,SUM(CAST(pt.Poly.STContains(dt.Toa_do) AS INT))  Soca from VungPhongToa pt, HoDuongTinh dt group by pt.ID) temp
    on maintoa.ID= temp.ID
    `, function (err, recordset) {

      if (err) console.log(err)
      // send records as a response
      return res.json(recordset);

    });
  });

}
module.exports = {
  getdataHoDuongTinh: getdataHoDuongTinh,
  tinhKC: tinhKC,
  getdataPhongToa: getdataPhongToa
};
