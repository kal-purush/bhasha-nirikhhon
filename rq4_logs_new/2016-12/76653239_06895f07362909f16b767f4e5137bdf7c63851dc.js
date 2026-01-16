const http = require('http');
const url = require('url')

const timeStamp = {
  getTime: function(time) {
    let date = new Date(time.query.hour);
    console.log(time);
    return {
      "date": date.getDate(),
    };
  }
}

const server = http.createServer(function(req, res) {
  let parsedUrl = url.parse(req.url, true)
  console.log(parsedUrl);
  let resolved = timeStamp.getTime;
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(resolved(parsedUrl)));
});

server.listen(4000);
console.log("port 4000");