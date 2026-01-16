const connect = require('connect');
const serveStatic = require('serve-static');

const port = process.env.PORT || 5000;
connect().use(serveStatic('static')).listen(port, function(){
    console.log('Server running on ' + port + '...');
});