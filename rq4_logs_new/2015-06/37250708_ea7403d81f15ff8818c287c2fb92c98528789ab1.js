'use strict';

var Hapi = require('hapi');
var server = new Hapi.Server({
  debug: {
    request: ['error']
  }
});
server.connection({
  port: process.env.PORT || 9000,
  routes: {
    cors: true
  }
});

// Set default node environment to development
process.env.NODE_ENV = process.env.NODE_ENV || 'development';

// Session
//require('./components/session')(server);

// Routing
require('./routes')(server);

// Logger
require('./components/logger')(server);

// Sync Database
var models = require('./models');
var config = require('./config');
models.sequelize.sync({force: true}).then(function () {

  // Insert seed data
  if(config.seedDB) { require('./config/seed').create(server); }

  // Start server
  server.start(function () {
    server.log('INFO', 'Server is running at ' + server.info.uri, process.env.NODE_ENV + ' mode');
  });
});

// For unit test
module.exports = server;