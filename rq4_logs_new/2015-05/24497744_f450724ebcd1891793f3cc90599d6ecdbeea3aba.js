var express = require('express');
var app = express();
var host = require('os').hostname();

app.set('port', (process.env.PORT || 3000))
app.use(express.static(__dirname + '/public'));

app.listen(app.get('port'), function() {
  console.log('App listening at http://%s:%s', host, app.get('port'));
})