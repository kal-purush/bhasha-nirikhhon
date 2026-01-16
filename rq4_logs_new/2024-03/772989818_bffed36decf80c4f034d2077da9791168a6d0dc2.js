const http = require('http');
const fs = require('fs');

const server = http.createServer((req, res) => {
if (req.url === '/') {
fs.readFile('index.html', (err, data) => {
if (err) {
res.writeHead(404, {'Content-Type': 'text/html'});
res.write('File not found');
} else {
res.writeHead(200, {'Content-Type': 'text/html'});
res.write(data);
}
res.end();
});
} else {
res.writeHead(404, {'Content-Type': 'text/html'});
res.write('Page not found');
res.end();
}
});

server.listen(3000, () => {
console.log('Server is running on http://localhost:3000');
});