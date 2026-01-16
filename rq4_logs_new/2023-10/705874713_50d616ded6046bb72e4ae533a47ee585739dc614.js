// Entry point
// We'll use HTTP API to expose
// player functionality
const dotenv = require('dotenv')
const Api = require('./src/api')

const DEFAULT_HTTP_PORT = '7001'

dotenv.config()
const api = new Api(process.env.PORT || DEFAULT_HTTP_PORT)
