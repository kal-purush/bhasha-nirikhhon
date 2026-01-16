const http = require("http");

const host = 'localhost';
const port = 8000;

const requireListener = function (req, res) {
  res.writeHead(200);
  res.end("My first server!");
};

const server = http.createServer(requireListener)
server.listen(port, host, () => {
  console.log(`Server is running on http://${host}:${port}`)
});