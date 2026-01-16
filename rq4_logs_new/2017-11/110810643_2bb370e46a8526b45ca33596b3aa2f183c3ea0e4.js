var http = require('http');
var qs = require('querystring');
var fs = require('fs');
var path = require('path');

// create a server object
http.createServer(function (req, res) {
    if (req.method === 'POST') {
        var data = [];
        var length = 0;
        req.on('data', function (chunk) {
            console.log('data')
            // receive data
            length += chunk.length;
            data.push(chunk);
        });
        req.on('end', function () {
            // joint data
            data = Buffer.concat(data, length);
            // analyze data
            var formData = qs.parse(String(data));
            req.body = formData;
            console.log(formData);
            res.write(String(data));
            res.end();
        })
    } else {
        console.log('inside home')
        var filename = path.resolve(__dirname, "form.html");
        console.log(filename);
        var content = fs.readFileSync(filename);
        console.log(String(content));
        res.write(String(content)); // write a response to the client
        res.end();  // end the response
    }
}).listen(8080);