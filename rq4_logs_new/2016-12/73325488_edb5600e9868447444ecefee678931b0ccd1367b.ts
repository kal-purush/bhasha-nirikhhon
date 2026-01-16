import {Router, Request, Response, NextFunction} from 'express';
import {forEach} from "@angular/router/src/utils/collection";
const crypto = require('crypto');
const mysql = require('mysql');
const userApi: Router = Router();

function getConnection() {
  return mysql.createConnection({
    host: 'eu-cdbr-west-01.cleardb.com',
    user: 'bd08922d88da6e',
    password: '8651bcec',
    database: 'heroku_4d519b9044708a5'
  });
}

function endConnection(connection) {
  connection.end(function (err) {
    console.log('Connection ended');
  });
}

function logError(err, query = 'Query') {
  console.log('Error while performing ' + query);
  console.log(err);
}

userApi.use((request: Request & { headers: { authorization: string } }, response: Response, next: NextFunction) => {
  const token = request.headers.authorization;

  let connection = getConnection();

  connection.query('SELECT * from session where token = ?', [token], function (err, rows, fields) {
    if(!err) {
      if(rows.length == 0) {
        return response.status(401).json({
          message: 'Invalid token, please Log in first'
        });
      } else {
        request.params.userId = rows[0].user_id;
        next();
      }
    } else {
      logError(err, 'Authorization');
    }
  });
});

userApi.delete('/odjava', function (request: Request, response: Response, next: NextFunction) {

  let connection = getConnection();

  let token = request.header('Authorization');

  connection.query('DELETE FROM session WHERE token = ?', [token], function (err, rows, fields) {
    if(!err) {
      response.status(200);
      response.json({odjava: true});
      endConnection(connection);
    } else {
      logError(err);
    }
  });

});

export {userApi}