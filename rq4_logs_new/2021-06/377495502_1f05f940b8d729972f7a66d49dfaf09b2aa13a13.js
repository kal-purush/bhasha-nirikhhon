const fs = require('fs');
const path = require('path');
const http = require('http');
const Router = require('./router');

const router = new Router();

router.get('/', (req, res) => {
  fs.readFile('./views/index.html',
    ((err, data) => {
      if (err) {
        res.writeHead(500);
        res.end('Internal server error');
      } else {
        res.setHeader('Content-type', 'text/html');
        res.writeHead(200);
        res.write(data);
        res.end();
      }
    }));
})

router.get('/about', (req, res) => {
  fs.readFile('./views/about.html',
    ((err, data) => {
      if (err) {
        console.log('sdjfkld');
        res.writeHead(500);
        res.end('Internal server error');
      } else {
        res.setHeader('Content-type', 'text/html');
        res.writeHead(200);
        res.write(data);
        res.end();
      }
    }));
})

const server = http.createServer(
  (req, res) => {
    router.__handle(req, res)
  },
);

server.listen(3000, () => {
  console.log('start started');
});