var http = require('http'),
    path = require('path'),
    serverConfigs = require('./app/configs/server.js'),
    app = require('./server').server;

app.listen(serverConfigs.port);
console.log('Server listening on port ' + serverConfigs.port + ';  NODE_ENV: ' + process.env.NODE_ENV);