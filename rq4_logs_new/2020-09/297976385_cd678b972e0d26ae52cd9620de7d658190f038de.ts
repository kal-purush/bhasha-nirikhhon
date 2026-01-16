import config, { IConfig } from 'config';
import mysql from 'mysql';

const dbConfig: IConfig = config.get('App.database');

export const connect = async (): Promise<void> =>
  await mysql
    .createConnection({
      host: dbConfig.get('HOST'),
      user: dbConfig.get('USER'),
      password: dbConfig.get('PASSWORD'),
      database: dbConfig.get('DB'),
    })
    .connect((error) => {
      // open the MySQL connection
      if (error) throw error;
      console.log('Successfully connected to the database.');
    });

// close the connection
//export const close = async (): Promise<void> =>
//  await mysql.end((error) => {
//    if (error) throw error;
//    console.log('Successfully close to the database.');
//  });