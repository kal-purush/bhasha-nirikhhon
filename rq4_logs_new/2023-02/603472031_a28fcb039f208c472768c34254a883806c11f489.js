const mysql = require('mysql');
import { get_credentials } from './query';
import Config from 'config';

let config = Config;

export const connection_db = mysql.createConnection(config.DATABASE_URI);

const queryConnectionCredentialsUser = (connection, resolve, username) => {
  return connection.query(get_credentials(username), function (err, result) {
    if (err) {
      resolve(403)
    };
    resolve(result);
  });
}

export const getDataByUser = (connection, username) => {
  return new Promise((resolve) => {
    queryConnectionCredentialsUser(connection, resolve, username);
  });
}