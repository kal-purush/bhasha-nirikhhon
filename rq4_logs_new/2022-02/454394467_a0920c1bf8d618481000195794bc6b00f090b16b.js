#!/usr/bin/node

const fs = require('fs');
const http = require('http');
const sh = require('child_process').execSync;


// Load configuration
let CONFIG = {
    timeout: 500
};
const configPath = `/etc/syscgijs/config.json`;
if (fs.existsSync(configPath)) {
    realConfig = fs.readFileSync(configPath);
    Object.keys(realConfig).map(function (keyname) {
        CONFIG[keyname] = realConfig[keyname];
    });
};


console.log(`Server starting.`);

http.createServer(function (req, res) {
    const cmdline = req.headers.cmdline
    const wwwroot = req.headers.wwwroot;
    const programPath = cmdline.split(' ')[0];
    if (fs.existsSync(programPath)) {
        res.writeHead(200, {
            'content-type': 'text/html'
        });

        // Inherit env from env and http request headers
        let localEnv = JSON.parse(JSON.stringify(process.env));
        localEnv.url = req.url;
        localEnv.wwwroot = wwwroot;
        console.log(`localEnv.url: ${localEnv.url}`);
        console.log(`localEnv.wwwroot: ${localEnv.wwwroot}`);
        console.log('----------------');
        
        // res.end('');
        // return 0;

        // Give response
        const stdout = sh(`${cmdline}`, {
            cwd: '/tmp',
            env: localEnv,
            timeout: req.headers.syscgi_timeout || CONFIG.timeout
        }).toString();
        res.end(stdout);
    } else {
        res.writeHead(500);
        res.end(`500: Server Internal Error`);
    };
}).listen(9234, '127.0.0.1');