const http = require('http');
const net = require('net');

const port = 3000;
const tcpPort = 1824;
const urls =
    {
        '/workers': getWorkers,
        '/workers/add': addWorker,
        '/workers/delete': deleteWorker
    }
const client = new net.Socket();
client.setEncoding('utf8');
client.connect(tcpPort, '127.0.0.1', () => {
    const server = http.createServer((req, resp) => {
        resp.setHeader('Content-Type', 'application/json');
        
        req.on('data', (chunk) => {
            let obj = JSON.parse(chunk);
            const handler = getHandler(req.url);
            handler(req, resp, (method) => {
                obj.method = method;
                client.write(JSON.stringify(obj));
            });
        });

        client.on('data', (data) => {
            resp.end(data);
        });
    });

    server.listen(port, '127.0.0.1', () => {
        console.log(`Listening on port ${port}`);
    });
});

function getHandler(url) {
    return urls[url];
}

function getWorkers(req, resp, cb) {
    cb('get');
}
function addWorker(req, resp, cb) {
    cb('create');
}
function deleteWorker(req, resp, cb) {
    cb('delete');
}