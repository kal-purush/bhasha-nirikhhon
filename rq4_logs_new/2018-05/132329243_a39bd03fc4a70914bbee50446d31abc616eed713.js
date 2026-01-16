const express = require('express');
const hbs = require('hbs');
const fs = require('fs');
var app = express();

hbs.registerPartials(__dirname + '/views/partials');
hbs.registerHelper('getFullYear',() => {
    return new Date().getFullYear();
});

hbs.registerHelper('screamIt',(text) => {
return text.toUpperCase();
});
app.set('view engine','hbs');

app.use((req,res,next) => {
    var now = new Date().toString();
    var log = `${now}: \nUrl: ${req.url} \nMethod: ${req.method}`;
    console.log(log);
    fs.appendFileSync('server.log',log+'\n');
next();
});

// app.use((req,res,next) => {
//     res.render('maintenance.hbs');
// });

app.use(express.static(__dirname + '/public'));
app.get('/',(request,response) => {
    // response.send('<h2> Hello Express!! </h2>');
    response.render('home.hbs',{
        pageTitle:'Home Page',
        welcomeMsg:'Welcome to my Brand new Express based website'
    })
});

app.get('/json',(request,response) => {
    response.send({
        'first-name':'Anubhav',
        'last-name':'Jaiswal'
    });
});

app.get('/bad',(req,res) => {
    res.send({
        errorMessage:'Unable to Fulfill the request'
    });
});

app.get('/about',(req,res) => {
    res.render('about.hbs',{
        pageTitle:'About Page'
    });
});

app.listen(3000,() => {
    console.log('Server is up on port 3000');
});