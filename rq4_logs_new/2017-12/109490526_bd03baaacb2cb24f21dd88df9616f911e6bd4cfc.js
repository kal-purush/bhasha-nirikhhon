// server.js
const express        = require('express');
const bodyParser     = require('body-parser');
const app            = express();
const port = 3000;

app.use(express.static('public'));
app.use(express.static('files'));
app.use(bodyParser.urlencoded({ extended: true }));
require('./routes/all_routes')(app);
app.set('view engine', 'ejs');
app.set('views', __dirname + '/views');
app.listen(port, () => {
  console.log('We are live on ' + port);
});