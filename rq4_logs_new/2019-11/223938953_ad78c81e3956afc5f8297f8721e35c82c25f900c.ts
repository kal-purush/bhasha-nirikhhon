import { config } from './config';

module.exports = {
    development: {
        username: config.DATABASE_USER || 'root',
        password: config.DATABASE_PASS || 'root',
        database: config.DATABASE_NAME || 'lms_db',
        host: config.DATABASE_HOST || '127.0.0.1',
        dialect: 'mysql'
    },
    test: {
        username: config.DATABASE_USER || 'root',
        password: config.DATABASE_PASS || 'root',
        database: config.DATABASE_NAME || 'lms_db',
        host: config.DATABASE_HOST || '127.0.0.1',
        dialect: 'mysql'
    },
    production: {
        username: config.DATABASE_USER || 'root',
        password: config.DATABASE_PASS || 'root',
        database: config.DATABASE_NAME || 'lms_db',
        host: config.DATABASE_HOST || '127.0.0.1',
        dialect: 'mysql'
    }
};