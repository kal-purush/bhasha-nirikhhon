const http = require("http")

const users = [
    {id : 1, name: 'Javid'},
    {id : 2, name: 'ibrahim'}
]

const server = http.createServer(
    (req, res) => {
        const url = req.url;
        const method = req.method;

        if (url === '/users' && method === 'GET') {
            res.writeHead(200, {'Content-Type' : 'application/json'})
            res.end(JSON.stringify(users))
        } else {
            res.writeHead(404, {'Content-Type' : 'text/plain'})
            res.end("Route Not Found")
        }
    }
)

server.listen(8080)