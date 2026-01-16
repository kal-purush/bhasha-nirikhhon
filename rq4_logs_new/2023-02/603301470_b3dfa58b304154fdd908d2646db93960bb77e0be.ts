const MONGO_CONNECTION_STRING = process.env.MONGO_CONNECTION_STRING || 'mongodb://localhost:27017/node-competition';
const admins = process.env?.admins || ['502480594']

export {
    MONGO_CONNECTION_STRING,
    admins
}