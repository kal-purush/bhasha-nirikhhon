const sessions = require('express-session')
const MongoDBSession = require('connect-mongodb-session')(sessions)
const uri = 'mongodb://localhost:27017/library'

const sessionstore = new MongoDBSession({
    uri: uri,
    collection: 'sessions'
})

module.exports = sessionstore;