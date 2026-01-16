// app.js
const express = require('express');
const app = express();

const expressLayouts = require('express-ejs-layouts');


app.set('views', __dirname + '/views');
app.set('view engine', 'ejs');
app.use(expressLayouts)


app.get('/', (req, res) => {
    res.send ('Hello')
  });

  app.get('/hopper', (req, res) => {
    res.send ('Hopper')
  });


app.get('/hopper/friend', (req, res) => {
    res.send (req.query)
  });


  app.get('/hopper/:name/:name3', (req, res) => {
    res.send (req.params)
  });


  app.get('/hopper/:name/', (req, res) => {
    res.send (req.params)
  });


app.get('/users/:username', (req, res) => {
    res.send(req.params);
    console.log (req.params)
  });


app.get('/books/:bookId', (req, res, next) => {
    res.send(req.params);
    console.log(req.params)
  });


app.get('/search', (req, res, next) => {
    res.send(req.query)
    console.log(req.query);
  });


  app.get('/', (req, res, next) => {
    res.render('index');
  });


app.listen(3000, () => console.log('App listening on port 3000!'));