var http = require('http');
var url = require('url');
var console = require('console');

var Router = require('routes');
var router = new Router();

var host = "127.0.0.1";
var port = "1337";

router.addRoute('/', root);
router.addRoute('/1/:ceiling?', problem1);

http.createServer(function (req, res) {
    var path = url.parse(req.url).pathname;
    var match = router.match(path);
    if (match) {
        res.writeHead(200, {'Content-Type': 'text/plain'});
        var result = match.fn(match.params);
        res.statusCode = 200;
        res.end("Result " + result);
    } else {
        res.writeHead(200, {'Content-Type': 'text/plain'});
        res.statusCode = 200;
        res.end("No route match");
    }
}).listen(port, host);

function root(req, res, match) {
    res.writeHead(200, {'Content-Type': 'text/plain'});
    res.statusCode = 200;
    res.end("root");
}

function problem1(params) {
    var result = 0;
    var ceiling = params.ceiling ? params.ceiling : 1000;

    for (var i = 0; i < ceiling; i++) {
        if (i % 3 == 0 || i % 5 == 0) {
            result += i;
        }
    }

    return result;
}

console.log('Server running at http://' + host + ':' + port);