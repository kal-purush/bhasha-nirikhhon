/*const http =require('http')

const server = http.createServer((req,res) => {

    res.status = 200;
    res.setHeader('Content-Type', 'text/plain');
    res.end('Hi there..!!');
})

server.listen(3032, () =>{
console.log('Server on port 3032')

});*/
// The above code is to set a simple Node Js server without express


//---------------- NEW CODE WHIT EXPRESS JS ----------------------
// - routing documentation http://expressjs.com/en/guide/routing.html
// - methods http://expressjs.com/en/4x/api.html


const { application } = require('express');
const express = require('express');
const morgan = require('morgan');
const app = express(); // app is the server

// A middleware is just a request handler, 
// which is executed before all other requests and works for all routes
// in other words, are used to process data before reaching the other paths
// ex.: to validate a user logging.

//function logger (req, res, next){
//    console.log(`Route Received: ${req.protocol}://${req.get('host')}${req.originalUrl}`);
//    next();
//}

// ------------- Settings ---------------------
// app.set to set envaironment variables
app.set('appName','Testing Express JS');
app.set('port','3000');
app.set('view engine','ejs')

// ------------ Middelwares -------------------
app.use(express.json());
app.use(morgan('dev'));
//app.use(logger)

// This "app.all" only works for "/user/*" routes


// --------------- Routes ---------------------
app.all('/user', (req, res, next) => {

    console.log('checking');
    next();
});

app.get('/', (req, res) => {
const data = [{name:'Peter'},{name:'Homer'},{name:"Will"}];
    res.render('index.ejs',{people: data});
});

//----------------------------------------------
app.get('/user',(req,res) => {
    res.json({

        username: 'Peter',
        surname: 'Parker',
        age: '18'
    });

});

//----------------------------------------------
app.post('/user/:id',(req,res) => {
    console.log(req.body);
    console.log(req.params);

    res.send('PETITION POST RECIVED');
    
});

//----------------------------------------------
app.put('/user/:id',(req,res) => {
    console.log(req.body)
    res.send(`User ${req.params.id} updated`);
        
});

//----------------------------------------------
app.delete('/user/:userId',(req,res) => {
    res.send(`User ${req.params.userId} deleted`);
            
});
// ------------------ end routes ----------------

app.use(express.static('public'));

app.listen(app.get('port'), () => {
    console.log(app.get('appName'));
   // before app.set port we used this console.log('Server on port 3032');
   console.log('Server on port',app.get('port'))
});
