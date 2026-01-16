import http = require('http');
import spinModule = require('./Profile_Demo/SpinFunc');
var n = 0;
var port: number = process.env.port || 1337;
http.createServer(function (req: http.ServerRequest, res: http.ServerResponse) {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    var s1 = 0, s2 = 0, s3 = 0;
    s1 = spinModule.spin(1000000);
    s2 = spinModule.spin(3000000);
    s3 = spinModule.spin(6000000);

    res.end(' n=' + n++ + ' s1=' + s1 + ' s2=' + s2 + ' s3=' + s3);

}).listen(port);