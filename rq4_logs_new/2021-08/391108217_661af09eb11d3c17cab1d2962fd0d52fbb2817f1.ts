import mysql from 'mysql2'
import Promise from 'bluebird'
import CONFIG from './config'
const { HOST, PORT, USER, PASSWORD, DATABASE } = CONFIG

export interface DbConfig {
  host: string
  port: number
  user: string
  password: string
  database: string
}

const config: Partial<DbConfig> = {
  host: HOST,
  port: +(PORT || 3306),
  user: USER,
  password: PASSWORD,
  database: DATABASE
}

const conn = mysql.createConnection(config)

const connection = Promise.promisifyAll(conn)

export default connection