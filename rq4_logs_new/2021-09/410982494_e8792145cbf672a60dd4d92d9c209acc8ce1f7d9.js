import mysql from 'mysql2/promise'
import {config} from './config'

export const connect = async() => {
  try{
    // const conn = await mysql.createConnection(config)
    // console.log('Data Base Connect')

    // const [rows] = await conn.query('SELECT * FROM movie')
    // console.log(rows)
    
    return await mysql.createConnection(config);
  }
  catch(err) {
    console.log(err)
  }
}
// connect();