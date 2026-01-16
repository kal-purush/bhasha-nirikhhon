var express = require('express');
var app = express();

app.set('view engine', 'ejs');

var bodyparser = require('body-parser');
//app.use(bodyparser.json);
app.use(bodyparser.urlencoded({extended:true}));

var table = require('./table.js');

app.use('/create',(req, res) => {
    var newtable = new table({
        x: req.body.x,
        y: req.body.y,
    });
    newtable.save((err) => {
        if(err){
            res.type('html').status(500);
            res.send('Error: ' + err)
        }
        else{
            res.render('created', {table: newtable})
        }
    });
});

app.get('/all',(req, res) => {
    table.find((err, allnumber) => {
        if(err) {
            res.type('html').status(500);
            res.send('Error: ' + err)
        }
        else if (allnumber.length == 0) {
            res.type('html').status(200);
            res.send('There are no recoed');
        }
        else {
            res.render('ShowAll', {values: allnumber});
        }
    });
});

app.use('/public', express.static('public'));

app.use('/',(req,res) => {
    res.sendFile(__dirname + '/pi.html');
});

app.listen(3000, ()=>{
    console.log('LIstening to 3000');
});