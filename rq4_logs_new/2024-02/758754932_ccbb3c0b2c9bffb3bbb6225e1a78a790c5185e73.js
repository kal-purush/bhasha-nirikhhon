// Gọi module 'http'
const http = require('http');

// Tạo server và lắng nghe cổng 3000
const server = http.createServer((req, res) => {
  // Thiết lập header của response
  res.writeHead(200, {'Content-Type': 'text/plain'});

  // Gửi nội dung "Hello, World!" về client
  res.end('Hello, World!\n');
  console.log('connected');
});

const port = process.env.PORT || 8080;
// Lắng nghe cổng 3000 và hiển thị thông báo khi server khởi động
server.listen(port, '127.0.0.1', () => {
  console.log('Server đang chạy tại http://127.0.0.1:/' + port);
});