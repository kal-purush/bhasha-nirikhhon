import { Pool } from 'pg';
import path = require('path');
let env = process.env.NODE_ENV || 'development';
let config = require(path.join(__dirname, '..', '..', 'config', 'database.config.json'))[env];
export abstract class PgPool{
    
    private static _pgPool: Pool;

    public static getPool(): Pool {
        if (!this._pgPool) {
            console.log('pg-pool create the first time');
            this._pgPool = new Pool({
                database: config.database,
                user: config.username,
                password: config.password,
                port: config.port,
                ssl: config.ssl,
                max: config.pool.max,
                min: config.pool.min,
                idleTimeoutMillis: config.idleTimeoutMillis
            });

            this._pgPool.on('error', (err, client) => {
                console.error(err.message);
                console.info(client);
            });

            // this._pgPool.on('connect', (client) => {
            //     console.info(client);
            // })
        }
        return this._pgPool;
    }
}