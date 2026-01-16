const express = require('express')
const session = require('express-session')
const mustache = require('mustache-express')
const bodyParser = require('body-parser')
const parseurl = require('parseurl')
const app = express()
var sess = ''

app.engine('mustache', mustache())
app.set('view engine', 'mustache')
app.use(express.static('public'))
app.use(bodyParser.urlencoded({ extended: false }))
app.use(session({
  secret: 'ssshhhhh',
  resave: false,
  saveUninitialized: true
}))

app.use(function (req, res, next) {
  var views = req.session.views

  if (!views) {
    views = req.session.views = {}
  }
  var pathname = parseurl(req).pathname
  views[pathname] = (views[pathname] || 0) + 1
  next()
})

app.get('/', function (req, res, next) {
  sess = req.session

  if (sess.username) {
    res.render('index', {
      user: sess.username,
      password: sess.password,
      views: (sess.views['/'] - 2)
    })
  } else {
    res.redirect('/login')
  }
})

app.get('/login', function (req, res, next) {
  res.render('login')
})

app.post('/login', function (req, res) {
  sess = req.session
  sess.username = req.body.username
  sess.password = req.body.password

  res.redirect('/')
})

app.post('/counter', function (req, res) {
  res.redirect('/')
})

app.listen(3000, function () {
  console.log('Started application')
})