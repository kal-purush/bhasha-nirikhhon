import 'dotenv/config';

const dbName = process.env.NODE_ENV == 'test' ? process.env.Test_DB_Name : process.env.DB_NAME;
const expiresIn = process.env.NODE_ENV == 'production' ? 60 * 30 : '60d'; // 30 minute or 60 day

const appConfig = {
  db: {
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT),
    password: process.env.DB_PASSWORD,
    dbName: dbName,
  },
  auth: {
    jwtExpiresIn: expiresIn,
    jwtKey: process.env.JWT_KEY,
    passwordPepper: process.env.PASSWORD_PEPPER,
    saltRound: parseInt(process.env.SALT_ROUND),
  },
  prot: parseInt(process.env.APP_PORT) || 3000,
};
export default appConfig;