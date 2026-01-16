var express = require('express');
var router = express.Router();

var mysql = require('mysql');
var config = require("../config");

/* GET home page. */
router.get('/', function(req, res, next) {

  let sqlConfig = {
      host: config.mysql.host,
      user: config.mysql.user,
      password: config.mysql.password,
      // database: config.mysql.database,
      port: config.mysql.port,
  };

  let connection = mysql.createConnection(sqlConfig);

  // console.log(connection);

  // connection.connect();
  connection.connect( (err, res) => {
    if(err)
    {
        // console.log("Error when connecting to db:", err);
        console.log("err code: ", err.code);
        console.log("err sqlMessage: ", err.sqlMessage);
    }
    else {
      // console.log(connection);
        console.log(res);
    }
  });

  // let sqlQuery = 'show tables';
  let sqlQuery = 'use test';

  connection.query(sqlQuery, (err, result) => {
    if(err) {
        console.log("err code: ", err.code);
        console.log("err sqlMessage: ", err.sqlMessage);
    }
    else {
        console.log(result);
    }
  });

  sqlQuery = "select * from order";
  connection.query(sqlQuery, (err, result) => {
      if(err)
      {
          console.log("err code: ", err.code);
          console.log("err sqlMessage: ", err.sqlMessage);
      }
      else {
          console.log(result);
      }
  });

  // sqlQuery = "desc orders";
  // connection.query(sqlQuery, (err, result) => {
  //     if(err)
  //         console.log("err: ");
  //     else {
  //         console.log(result);
  //     }
  // });
  //
  // sqlQuery = "show tables";
  // connection.query(sqlQuery, (err, result) => {
  //     if(err)
  //         console.log("err: ");
  //     else {
  //         console.log(result);
  //     }
  // });
  //
  // connection.on('error', err => {
  //   console.log(err.code);
  // });

  // connection.end(err => {
  //   if(err)
  //     throw err;
  //   console.log('------------connection end succeed!--------------')
  // })

});

module.exports = router;