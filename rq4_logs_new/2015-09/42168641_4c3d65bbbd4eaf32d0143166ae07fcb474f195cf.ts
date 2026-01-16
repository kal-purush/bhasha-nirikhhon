import express = require('express');
import mongoose = require('mongoose');
import config = require('./config');
var restIO: any = require('rest-io');
var app = express();
restIO(app, {
  resources: __dirname + '/resources'
});
var connect = config.getMongoConfig();
var serverConfig = config.getServerConfig();
mongoose.connect(connect.uri, connect.options);

app.listen(serverConfig.port, () => {
  console.log('Server has started under port: ' + serverConfig.port);
});

export = app;
