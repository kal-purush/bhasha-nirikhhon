const http = require('http');
const url = require('url');
const fs = require('fs');

http.createServer(function (req, res) {
    const q = url.parse(req.url, true);
    let filename = "." + q.pathname+".html";
    if(q.href == "/"){
        filename = "./index.html"
    }
    else if(filename != "./index.html"&& filename != "./about.html"&& filename != "./contact-me.html"&& filename != "./404.html"){
        filename = "./404.html"
    }
    fs.readFile(filename, function(err, data) {
        res.writeHead(200, {'Content-Type': 'text/html'});
        res.write(data);
        return res.end();
    });
}).listen(8080);