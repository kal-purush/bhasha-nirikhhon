const http = require('http');
const ReactDOMServer = require('react-dom/server');
const Template = require('./tmpl');
const nodeStatic = require('node-static');

require('node-jsx').install();
const AppElement = require('./react/app-element');

const { PORT = 3000 } = process.env;
const tmpl = Template.init('view.tmpl.html');

const file = new nodeStatic.Server('./public');

const server = http.createServer((req, res) => {

  if (req.url.indexOf('/dist') === 0) {
    file.serve(req, res);
    return;
  }

  let appHTML = '';
  const stream = ReactDOMServer.renderToNodeStream(AppElement);
  stream.on('data', c => {
    appHTML += c;
  });
  stream.on('end', () => {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(
      Template.compile(tmpl, {
        TITLE: 'foo title',
        APP_HTML: appHTML,
        JS_BUNDLE: '/dist/main.js',
        CSS_BUNDLE: '/dist/main.css'
      })
    );
  });
});

server.listen(PORT, () => {
  console.log(`Listening on ${PORT}`);
});