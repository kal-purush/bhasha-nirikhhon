import express from 'express'

const server = express()

server.get('*', (request, response) => response.send('hello andrew').end())

const { env: { IP = 'http://localhost', PORT = 3000 } } = process

const callback = () => console.log(`listening on ${IP}:${PORT}`)

server.listen(PORT, callback)