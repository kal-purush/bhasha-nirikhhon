var express = require('express');
var reload = require('reload');
var app = express();
//var dataFile = require('./data/data.json');

app.set('port', process.env.PORT || 9000 );
//app.set('appData', dataFile);
app.set('views', 'app/views');

app.locals.siteTitle = 'E-Commerce Product Store';

app.use(express.static('app/public/angular-app'));

var server = app.listen(app.get('port'), function() {
  console.log('Listening on port ' + app.get('port'));
});

reload(server, app);