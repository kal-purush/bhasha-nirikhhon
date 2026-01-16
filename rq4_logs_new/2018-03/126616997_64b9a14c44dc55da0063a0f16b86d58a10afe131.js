module.exports = {
    secret: process.env.JWT_HASH,
    database: process.env.MONGO_URI,
    gKey: process.env.GOOGLE_KEY,
    port: process.env.PORT,
    baseURL: process.env.APP_URL
};