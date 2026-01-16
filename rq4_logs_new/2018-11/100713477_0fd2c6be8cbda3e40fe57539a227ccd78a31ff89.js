const http = require('http');
const { URL } = require('url');
const querystring = require('querystring');
const { StringDecoder } = require('string_decoder')
const fs = require('fs');
const multiparty = require('multiparty');

const decoder = new StringDecoder('utf8');

const server = http.createServer((req, res) => {

  let buffer = '';
  // req.on('data', (chunk) => {
  //   buffer += chunk;
  // });

  // req.on('end', () => {
  //   const parsedUrl = new URL(req.url, 'http://localhost:8080/');

  //   res.setHeader('Content-Type', 'application/json');
  //   res.write(JSON.stringify({
  //     href: parsedUrl.href,
  //     host: parsedUrl.host,
  //     origin: parsedUrl.origin,
  //     port: parsedUrl.port,
  //     protocol: parsedUrl.protocol,
  //     params: querystring.parse(parsedUrl.search),
  //     data: querystring.parse(buffer),
  //   }));
  //   res.end();

  // });

  var form = new multiparty.Form();

  form.on('file', (name, file) => {
    res.setHeader('Content-Type', 'application/json');
    res.write(JSON.stringify({
      name,
      file,
    }));
    res.end();
  });

  form.parse(req);

});


server.listen('8080', () => {
  console.log('Server ins running on 8080 port')
});