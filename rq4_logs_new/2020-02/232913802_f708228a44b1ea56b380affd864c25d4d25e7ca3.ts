const http = require('http');

const fs = require('fs');
const path = require('path');

const server = http.createServer((request, response) => {
  let filepath = path.join(
    __dirname,
    'public',
    request.url === '/' ? 'index.html' : request.url
  );

  const ext = path.extname(filepath);

  let contentType = 'text/html';

  if (!ext) {
    filepath += '.html';
  } else {
    switch (ext) {
      case '.css':
        contentType = 'text/css';
        break;
      case '.js':
        contentType = 'text/javascript';
        break;

      default:
        break;
    }
  }

  // console.log(filepath);

  fs.readFile(filepath, (err, data) => {
    if (err) {
      // throw err;
      response.writeHead(500, {
        'Contentent-type': 'text/html'
      });

      response.end('<h1>error</h1>');
    } else {
      response.writeHead(200, {
        // "Contentent-type": "text/plain" // текст- - теги не парсятся
        'Content-Type': contentType
      });

      response.end(data);
    }
  });
});

// console.log(process.argv);
const PORT = process.env.PORT || 3000;

// Из соображений безопасности в большинстве операционных систем запрещено прослушивать стандартный порт НТТР (80) без запроса на повышение прав. Фактически повышенные права необходимы для прослушивания любого порта ниже 1024. Для целей разработки и отладки обычно используются порты выше 1024 и выбирают такие номера, как 3000, 8000, 3030 и 8080, поскольку их легче запомнить:

server.listen(PORT, () => {
  console.log(`server has been started on ${PORT} ...`);
});