const express = require('express');
const bodyParser = require('body-parser');
const session = require('express-session');
const app = express();

app.use(bodyParser.urlencoded({extended: true}));

app.use(session({secret: '098234lkjsadf098234kjhsdf097834kjhsdf'}));
app.use(function(req, res, next){
    res.locals.user = req.session.user;
    next();
})
app.set('view engine', 'ejs');
app.use(express.static(__dirname + "/static/"));
app.set('views', __dirname + "/views/");


app.get('/', function(req, res){
    res.render('index');
});

app.post('/guess', function(req, res){
    //process guess here... 
    console.log(req.body);
    console.log("*****************************************************");

    //Check if AI already has a number. 
    if(req.body.ainumber){
        let ainumber = req.body.ainumber;
        let guess = req.body.guess;
        let status = none;
    }
    else {
        ainumber = Math.floor(Math.random()*100)+1
        let guess = req.body.guess; 
        let status = none; 
    }   
    if(guess > ainumber){
            status = 'high';
    }
    else if(guess < ainumber){
        status = 'low';
    }
    else if(guess == ainumber){
        status = 'win';
    }

    results = {
        'ainumber': ainumber,
        'guess': guess, 
        'status': status
    }

    res.render('index', results);
})

app.post('/reset', function(req, res){
    // req.session.destroy(function(){
    //     req.session = null;
    //     res.clearCookie('express.sid', {path: '/'});
    //     res.redirect('/');
    })

});

port = 8000; 
app.listen(port, function(){
    console.log("Listening on Port: ", port);
});