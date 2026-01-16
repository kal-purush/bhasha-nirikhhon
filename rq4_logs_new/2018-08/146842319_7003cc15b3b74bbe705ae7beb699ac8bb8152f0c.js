const server = require('./src/server')
const env = process.env.NODE_ENV || 'development'
const port = process.env.PORT || 3000
server.listen(port)
console.log(`Environment ${env}`)
console.log(`Server is running in port ${port}`)