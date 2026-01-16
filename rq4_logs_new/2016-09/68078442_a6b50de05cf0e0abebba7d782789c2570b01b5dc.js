var express = require('express')
var hbs = require('express-handlebars')
var bodyParser = require('body-parser')
var path = require('path')

var db = require('./db')

var app = express()

app.engine('hbs', hbs())
app.set('view engine', 'hbs')
app.set('views', path.join(__dirname, 'views'))

app.get('/list', function (req, res) {
  db.getWombles()
    .then(function (wombles) {
      var vm = {
        wombles: wombles
      }
      console.log(vm)
      res.render('list', vm)
    })
    .catch(function (err) {
      console.error(err.message)
      res.status(500).send("Can't display users!")
    })
})

app.get('/view', function (req, res) {

})

app.listen(3000, function () {
  console.log('listening on port 3000')
})