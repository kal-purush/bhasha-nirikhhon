// vim:set expandtab sw=2 sts=2:
var connect = require('connect'),
  serveStatic = require('serve-static');

connect().use(serveStatic(__dirname)).listen(8080, function() {
  console.log('Server running on 8080...');
});