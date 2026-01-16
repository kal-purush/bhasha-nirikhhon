var express = require('express');
var ejs = require('ejs');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');

var db = require(__dirname+'/helpers/db');
//var routes = require('./routes/index');
//var users = require('./routes/users');

var models = {
	contact:require(__dirname+'/models/contact'),
	contactBio:require(__dirname+'/models/contactBio'),
	contactLocation:require(__dirname+'/models/contactLocation'),
	contactNumber:require(__dirname+'/models/contactNumber'),
	contactSkill:require(__dirname+'/models/contactSkill'),
	contactBlog:require(__dirname+'/models/contactBlog'),
}

var controllers = {
	homePages:require(__dirname+'/web/controllers/homePages'),
	
	contacts:require(__dirname+'/controllers/contacts'),
	contactBios:require(__dirname+'/controllers/contactBios'),
	contactLocations:require(__dirname+'/controllers/contactLocations'),
	contactNumbers:require(__dirname+'/controllers/contactNumbers'),
	contactSkills:require(__dirname+'/controllers/contactSkills'),
	contactBlogs:require(__dirname+'/controllers/contactBlogs'),
}

var app = express();
app.use(express.static(path.join(__dirname + '/public')));

app.models = {
	contact:new models.contact(db),
	contactBio:new models.contactBio(db),
	contactLocation:new models.contactLocation(db),
	contactNumber:new models.contactNumber(db),
	contactSkill:new models.contactSkill(db),
	contactBlog:new models.contactBlog(db),
}

app.controllers = {
	homePages:new controllers.homePages(app, express),
	
	contacts:new controllers.contacts(app, express),
	contactBios:new controllers.contactBios(app, express),
	contactLocations:new controllers.contactLocations(app, express),
	contactNumbers:new controllers.contactNumbers(app, express),
	contactSkills:new controllers.contactSkills(app, express),
	contactBlogs:new controllers.contactBlogs(app, express),
}

// view engine setup
app.set('views', path.join(__dirname, 'views'));

app.engine('html', require('ejs').renderFile);
app.set('view engine', 'jade');
app.set('view engine', 'html');

// uncomment after placing your favicon in /public
app.use(favicon(__dirname + '/public/faviconME.ico'));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
//app.use(express.static(path.join(__dirname, 'public')));

//app.use('/', routes);
//app.use('/users', users);

for (var x in app.controllers) {
	console.log(x)
	app.controllers[x].init()
}

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
  app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error.jade', {
      message: err.message,
      error: err
    });
  });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
  res.status(err.status || 500);
  res.render('error.jade', {
    message: err.message,
    error: {}
  });
});


module.exports = app;