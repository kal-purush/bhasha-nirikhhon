import dotenv from 'dotenv'; 
dotenv.config(); 
import mysql from 'mysql2'

export class DatabaseUtility {
    private queryFormat: any

    constructor() {
        this.queryFormat = (query: string, values: Array<string>) => {
            if (!values) return query
            return query.replace(/\:(\w+)/g, (txt, key) => {
                return values.hasOwnProperty(key) ? mysql.escape(values[key]) : txt
            })
        }
    }

    private connect(callback: (dbc: mysql.Connection) => Promise<any>): Promise<any> {
        return new Promise((resolve, reject) => {
            const dbc = mysql.createConnection({
                host: process.env.DB_HOST,
                user: process.env.DB_USER,
                password: process.env.DB_PASS,
                database: process.env.DB_NAME
            })
            dbc.connect((error) => {
                if (error) {
                    reject(error)
                } else {
                    dbc.config.queryFormat = this.queryFormat
                    callback(dbc)
                        .then(result => resolve(result))
                        .catch(error => reject(error))
                        .finally(() => dbc.end())
                }
            })
        })
    }

    private sendQuery(dbc: mysql.Connection, query: string, option?: any): Promise<any> {
        return new Promise((resolve, reject) => {
            dbc.query(query, option, (error, results) => {
                if (error) {
                    reject(new Error(`SQL error: ${query}`))
                } else {
                    resolve(results)
                }
            })
        })
    }

    private async sendQueries(dbc: mysql.Connection, queries: Array<{ query: string, option?: any }>) {
        for (var i = 0; i < queries.length; i++) {
            await this.sendQuery(dbc, queries[i].query, queries[i].option)
        }
    }

    query(query: string, option?: any): Promise<any> {
        return this.connect((dbc: mysql.Connection) => this.sendQuery(dbc, query, option))
    }

    queries(queries: Array<{ query: string, option?: any }>): Promise<any> {
        return this.connect((dbc: mysql.Connection) => this.sendQueries(dbc, queries))
    }

    ensureTableExists(tableName: string, createTableSql: string): Promise<any> {
        const checkTableExistsQuery = `SHOW TABLES LIKE '${tableName}'`;
        return this.query(checkTableExistsQuery).then(results => {
            if (results.length === 0) {
                return this.query(createTableSql);
            }
        });
    }
}