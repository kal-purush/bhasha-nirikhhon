const http = require("http"); // allows access to module

const server = http.createServer(); // attaches createServer method to const
server.on("connection", () => {
  console.log("Connection established.");
}); // use the 'on' method to listen for connection event

server.listen(3000); // starts listener to port 3000, when it is triggered 'connection established' will be logged