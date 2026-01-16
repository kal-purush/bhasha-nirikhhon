import dotenv from 'dotenv';
dotenv.config();

export let port = process.env.PORT || 3000;

export let database: DBConfig = {
    host: process.env.DB_HOST || 'localhost',
    port: Number(process.env.DB_PORT || 3306),
    name: process.env.DB_NAME || 'test',
    user: process.env.DB_USER || 'username',
    password: process.env.DB_PASSWORD || 'password',
    connection_limit: Number(process.env.DB_CONNECTION_LIMIT || 10),
    queue_limit: Number(process.env.DB_QUEUE_LIMIT || 0)
};

if (
    isNaN(database.port) ||
    isNaN(database.connection_limit) ||
    isNaN(database.queue_limit)
) {
    console.log('Invalid database .env configuration. Exiting...');
    process.exit(1);
}

export interface DBConfig {
    host: string;
    port: number;
    name: string;
    user: string;
    password: string;
    connection_limit: number;
    queue_limit: number;
}