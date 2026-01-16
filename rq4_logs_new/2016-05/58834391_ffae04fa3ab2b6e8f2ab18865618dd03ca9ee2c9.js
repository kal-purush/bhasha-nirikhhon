var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var index = require('./routes/toindex');
var calculate = require('./routes/calculator');

app.set('port', (process.env.PORT || 5000));

app.use(bodyParser.urlencoded({extended: true}));

//ROUTES
app.use('/calculator', calculate);

app.use('/', index);





app.listen(app.get('port'), function() {
  console.log('Server is ready on port:' + app.get('port'));
});