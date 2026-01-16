const express = require('express');
const multer = require('multer');
const ejs = require('ejs');
const path = require('path');

const app = express();

app.set('view engine', 'html');
app.engine('html', ejs.renderFile);

app.use(express.static(__dirname + '/public'));


app.get('/', (req, res) => {
  res.render('index');
})

app.listen(3000, () => console.log('Listening on 3000..'));