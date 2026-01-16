//imports
var alert=require('alert-node')
var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');

//routes
//var indexRouter = require('./routes/index');
var login=require('./routes/login');
var forgot=require('./routes/forgot');
var signup=require('./routes/signup');

//var aboutus = require('./routes/about');
//var usersRouter = require('./routes/users');

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');

//mongoose setup
const mongoose = require('mongoose');
//You need to have an account created ib mongoose and use connect url directly
mongoose.connect('mongodb+srv://node-js:node-js@cluster0.aj7vv.mongodb.net/node-js?retryWrites=true&w=majority');
const db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error:'));
db.once('open', function (callback) {
  console.log('connect');
});

//default
app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));
//app.use(express.static(__dirname + '/public'));

//Tables Schema
var userSchema = new mongoose.Schema({
  username: { type: String }
  , name: { type: String }
  , password: {type: String}
  , mobile_number: {type: Number}
});
var itemSchema=new mongoose.Schema({
    itemid:{type : String},
    itemname:{type : String},
    itemprice:{type : Number}
});
var cartSchema = new mongoose.Schema({
    itemid:{type : String}
    ,username: {type:String}
});

var Item = mongoose.model('Item', itemSchema);
var Cart = mongoose.model('Cart', cartSchema);
var User = mongoose.model('User', userSchema);
var username;
//app.use('/', indexRouter);

app.use('/', login);
app.use('/forgot', forgot);
app.use('/sign-up', signup);

app.post('/check', function (req,res) {
    //alert("Invalid username or password");
    var userdetails= User.find({username:req.body.username, password:req.body.password },function(err, items) {
        if (err) return alert("Invalid username or password");
    });
    username=userdetails.username;
    res.render('home',{username:userdetails.username, nameofuser:userdetails.name});
});

app.post('/password-changed', function (req,res) {
    //alert("Invalid username or password");
    var user = req.body.username;
    var psw = req.body.psw;
    var pswrepeat = req.body.pswrepeat;
    if(psw==pswrepeat){
        const userdetails1=User.find({username:req.body.username},function(err, items) {
            if (err) return alert("Invalid username or password");
        });
        userdetails1.set({
            password:psw
        });
        userdetails1.save();;
    }
    else{
        alert("Entered passwords don't match");
    }
});

app.post('/sign-up-complete', function (req,res) {
    var username = req.body.username;
    var psw = req.body.psw;
    var pswrepeat = req.body.pswrepeat;
    if(psw.localeCompare(pswrepeat)){
        const userdetails1=User.find({username:req.body.username},function(err, items) {
            if (err) {
                alert("Entered username already exists");

            }
            else{
                var user=new User({
                    username: username
                    , password: psw
                    , name: req.body.name,
                    mobile_number: req.body.mobile
                });
                user.save(function(err, use) {
                    if (err) return console.error(err);
                    alert("Account created");
                });
                res.render('home',{username:userdetails.username, nameofuser:req.body.name})
            }
        });
    }
    else{
        alert("Entered passwords don't match");
    }
});
//app.use('/about', aboutus);
//app.use('/users', usersRouter);

app.post( '/shopping-samsung',
    function (req, res ){
        var cart = new Cart({
            username: username
            , itemid:"samsung"
        });
        cart.save(function(err, use) {
            if (err) return console.error(err);
            alert("Samsung added");
        });
    }
);
app.post( '/shopping-oneplus',
    function (req, res ){
        var cart = new Cart({
            username: username
            , itemid:"oneplus"
        });
        cart.save(function(err, use) {
            if (err) return console.error(err);
            alert("Item added");
        });
    }
);
app.post( '/shopping-redmi',
    function (req, res ){
        var cart = new Cart({
            username: username
            , itemid:"redmi"
        });
        cart.save(function(err, use) {
            if (err) return console.error(err);
            alert("Item added");
        });
    }
);
app.post( '/shopping-oppo',
    function (req, res ){
        var cart = new Cart({
            username: username
            , itemid:"oppo"
        });
        cart.save(function(err, use) {
            if (err) return console.error(err);
            alert("Item added");
        });
    }
);
app.post( '/shopping-vivo',
    function (req, res ){
        var cart = new Cart({
            username: username
            , itemid:"vivo"
        });
        cart.save(function(err, use) {
            if (err) return console.error(err);
            alert("Item added");
        });
    }
);
app.post( '/shopping-apple',
    function (req, res ){
        var cart = new Cart({
            username: username
            , itemid:"apple"
        });
        cart.save(function(err, use) {
            if (err) return console.error(err);
            alert("Item added");
        });
    }
);


//login


/*
app.post( '/create',
    function (req, res ){
        var shop = new Shop({
            username: req.body.id
            , password: 'Not Bought'
        });
        shop.save(function(err, use) {
            if (err) return console.error(err);
            alert("Item added");
        });
    }
);

app.get('/read' ,
    function (req, res ){
        var shop = new Shop({
            itemname: req.body.itemname
            , status: 'Not Bought'
        });
        Shop.find({'status':'Not Bought' },function(err, items) {
            if (err) return console.error(err);
            console.dir(shop);
            res.render('read', {items:items});
        });
    }
);

app.post( '/update',function (req, res ){

  var query = {"itemname": req.body.itemname};
  var update = {"status":"Bought"};
  var options = { multi: true};
  Shop.findOneAndUpdate(query, update, options, function(err, result) {
    if (err) return console.error(err);
    console.dir(result);
    res.render('message', {
      message: 'Item updated ' + result
    });
  });
});


app.post( '/delete',
    function (req, res ){

      Shop.find({ itemname:req.body.itemname }).remove().exec(function callback (err, numAffected) {
        if (err) return console.error(err);
        console.dir(numAffected);
        alert("Item removed from cart");
        //res.render('message', {
        //  message: 'Item updated ' + numAffected
        //});
      });

    });
// about page


// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});
*/
module.exports = app;

