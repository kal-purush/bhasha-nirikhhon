'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const mime = require('mime');
const pify = require('pify');
const fsp = pify(fs);

const DEBUG = process.env.DEBUG == 1;
const port = process.argv[2]|0 || 8080;

http.createServer(function(req, res){
    var netpath = path.resolve('/', req.url);
    var filepath = path.join(process.cwd(), netpath);
    (new Promise((resolve) => {
        fs.exists(filepath, resolve);
    }))
    .then(status => {
        if (! status) {
            res.statusCode = 404;
            res.end('Nothing found');
            return;
        }

        return fsp.stat(filepath).then(stat => {
            if (stat.isDirectory()) {
                return fsp.readdir(filepath).then(files => {
                    res.setHeader('content-type', 'text/plain');
                    res.end(files.join('\n'));
                });
            } else {
                res.setHeader('content-type', mime.lookup(req.url));
                fs.createReadStream(filepath).pipe(res);
            }
        })
    })
    .catch(error => {
        res.statusCode = 500;
        res.end(DEBUG ? error.stack : error.message);
    });
}).listen(port, function(){
    console.log('Server is started on 0.0.0.0:%s', port);
});