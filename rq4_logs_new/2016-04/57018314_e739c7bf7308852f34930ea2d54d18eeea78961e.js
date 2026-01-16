var express = require('express');
var app = express();
/*
app.get('/', function (req, res) {
  res.send('Hello World!');
});
*/
app.get('/webhook/', function (req, res) {
  if (req.query['hub.verify_token'] === 'whitewalkers') {
    res.send(req.query['hub.challenge']);
  }
  res.send('Error, wrong validation token');
})

app.listen(8080, function () {
  console.log('Example app listening on port 8080!');
});
