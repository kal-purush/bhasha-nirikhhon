var session = require('express-session')

app.use(session({
  secret: 'keyboard cat',
  resave: false,
  saveUninitialized: true,
  cookie: { secure: false } //secure:true --> session between multiple routes not working
}))


req.session.user = 'some user data to save'
if(req.session || req.session.user){
  //things goes here
}