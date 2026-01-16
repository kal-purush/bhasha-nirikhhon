var axios = require("axios");
var http = require("http");

const server = http
  .createServer(function (request, response) {
    if (request.url === "/json") {
      axios
        .get("http://www.omdbapi.com/?s=star+wars&apikey=cbbc6750")
        .then((res) => {
          response.writeHead(200, { "content-type": "text/json" });
          const body = JSON.stringify(res.data);
          response.write(body);
          response.end();
        })
        .catch((err) => {
          // Handle error if axios fetching fails
          response.writeHead(500, { "content-type": "text/plain" });
          response.end("Internal Server Error");
        });
    } else if (request.url === "/taulukko") {
      axios
        .get("http://www.omdbapi.com/?s=star+wars&apikey=cbbc6750")
        .then((res) => {
          response.writeHead(200, { "content-type": "text/html" });
          const body = parse(res.data);
          response.write(body);
          response.end();
        })
        .catch((err) => {
          // Handle error if axios fetching fails
          response.writeHead(500, { "content-type": "text/plain" });
          response.end("Internal Server Error");
        });
    } else {
      response.write("Joku muu sivu");
      response.end();
    }
  })
  .listen(3001);

function parse(data) {
  console.log("Parse");
  var html = "<table border='1'>";
  for (var i = 0; i < 10; i++) {
    html += "<tr>";
    html += "<td>" + data.Search[i].Title + "</td>";
    html += "<td>" + data.Search[i].Type + "</td>";
    html += "</tr>";
  }
  html += "</table>";
  console.log(html);
  return html;
}