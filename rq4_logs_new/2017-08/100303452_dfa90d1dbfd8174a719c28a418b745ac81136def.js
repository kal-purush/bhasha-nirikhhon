const express = require('express');
const handlebars = require('express-handlebars');
const session = require('express-session');
const validator = require('express-validator');
const bodyparser = require('body-parser');
const app = express();

app.engine('handlebars', handlebars());
app.set('views', './views');
app.set('view engine', 'handlebars');

app.use(
  session({
    secret: 'sbjmr',
    resave: false,
    saveUninitialized: true
  })
);

app.use(express.static('public'));

app.use(bodyparser.json());
app.use(bodyparser.urlencoded({
  extended: false
}));

app.use(validator());

let info = [{
  username: 'user1',
  password: 'password1'
},
{
  username: 'user2',
  password: 'password2'
},
{
  username: 'user3',
  password: 'password3'
},
{
  username: 'user4',
  password: 'password4'
}]

app.get('/', function(req, res) {
  if (!req.session.person) {
  res.redirect('/login')
} else {
 res.render('main', {
   username: req.session.person
 });
}
})

app.get('/login', function(req, res) {
  res.render('login')
});

app.post('/login', function(req, res) {
  let person = req.session.body;

  req.checkBody('username', 'Username is required').notEmpty();
   req.checkBody('password', 'Password please!').notEmpty();

   let errors = req.validationErrors();

   if (errors) {
     res.render('login', {errors: errors});
   } else {
     let users = info.filter(function(userCheck) {
       return userCheck.username === req.body.username;
     });


    if (users.length === 0) {
      let wrong = "User not found. Please create an account."
      res.render('login', {wrongmessage: wrong});
      return;
    }

    let user = users[0];


    if (user.password === req.body.password) {
      req.session.person = user.username;
      res.redirect('/');
    } else {
        let wrong = "not the password"
        res.render('login', {something: wrong});
      }
  }
});

//see if it works at all
app.listen(3000, function() {
  console.log('something is happening I promise')
});