const http = require("http");
const port = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  if (req.url === "/") {
    res.end("Home");
  } else {
    res.end("Hello3331");
  }
});
server.listen(port, () => {
  console.log("listening on " + port);
});