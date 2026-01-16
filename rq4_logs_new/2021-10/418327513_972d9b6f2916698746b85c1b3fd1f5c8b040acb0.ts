import {Sequelize} from "sequelize-typescript";
import * as dotenv from "dotenv";

dotenv.config();

const databaseCredentials = {
    development: {
        username: process.env.CARD_PLATFORM_DB_USER,
        password: process.env.CARD_PLATFORM_DB_PASSWORD,
        host: process.env.CARD_PLATFORM_DB_HOST,
        database: process.env.CARD_PLATFORM_DB_DEV_DB_NAME
    },
    test: {
        username: process.env.CARD_PLATFORM_DB_USER,
        password: process.env.CARD_PLATFORM_DB_PASSWORD,
        host: process.env.CARD_PLATFORM_DB_HOST,
        database: process.env.CARD_PLATFORM_DB_TEST_DB_NAME
    },
    production: {
        username: process.env.CARD_PLATFORM_DB_USER,
        password: process.env.CARD_PLATFORM_DB_PASSWORD,
        host: process.env.CARD_PLATFORM_DB_HOST,
        database: process.env.CARD_PLATFORM_DB_PROD_DB_NAME
    }
}

const {username, password, host, database} =
    process.env.NODE_ENV === 'production' ? databaseCredentials.production :
        process.env.NODE_ENV === 'test' ? databaseCredentials.test :
            databaseCredentials.development

const sequelize: Sequelize = new Sequelize(
    database,
    username,
    password, {
        host,
        dialect: 'mysql',
        port: 3306,
        dialectOptions: {
            multipleStatements: true,
        },
        pool: {
            max: 5,
            min: 0,
            idle: 10000
        },
        logging: false
    });


export default sequelize