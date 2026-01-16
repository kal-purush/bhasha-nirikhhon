var http = require('http');
var router = require('./lib/router');
var routes = require('./routes');


var port = process.env.PORT ||
  process.argv[2] ||
  3000;
var host = 'localhost';


var server = http.createServer(router.handle);


server.listen(port, host, () => {
  console.log(`Listening: http://${ host }:${ port }`);
});

