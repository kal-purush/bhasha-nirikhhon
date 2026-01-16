const fs = require('fs')
const path = require('path')
const cron = require('node-cron')
const http = require('http')
const createDigest = require('./lib/create-digest')
const { feedPath } = require('./lib/get-feed')
const config = require('./public/config.json')

const iconPath = path.join(process.cwd(), 'icon.png')

const PORT = 8080

if (!fs.existsSync(feedPath)) {
  console.log('Creating initial digest')
  createDigest('Initial')
}

config.digests.forEach(digest => {
  console.log(`Scheduling ${digest.name} digest (${digest.cron})`)

  cron.schedule(digest.cron, () => {
    console.log(`Creating ${digest.name} digest`)
    createDigest(digest.name)
  })
})

http
  .createServer((req, res) => {
    console.log(`${req.method} ${req.url}`)

    if (req.url === '/feed.json') {
      res.setHeader('Content-Type', 'application/json')
      fs.createReadStream(feedPath).pipe(res)
    } else if (req.url === '/icon.png') {
      res.setHeader('Content-Type', 'image/png')
      fs.createReadStream(iconPath).pipe(res)
    } else {
      res.writeHead(404).end()
    }
  })
  .listen(PORT, () => {
    console.log(`Listening on port ${PORT}`)
  })