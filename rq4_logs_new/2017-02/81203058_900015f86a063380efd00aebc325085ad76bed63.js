var fs = require('fs');
var http = require('http');
var express = require('express');

//db연결
var mongoose = require("mongoose");

var db_config  = require('./config/dbinfo.json');
mongoose.connect(
    "mongodb://"+db_config.id+":"+db_config.pwd+"@ds129469.mlab.com:29469/nic"
);

var db = mongoose.connection;
db.once("open", function(){
    console.log("DB connected");
});
db.on("error", function(err){
    console.log("DB Error : ", err);
});


var app = express();
var server = http.createServer(app);

app.get('/tracker', function (req, res) {
   fs.readFile('Tracker.html', function (error,data) {
       res.send(data.toString());
   });
});

app.get('/observer', function (req, res) {
    fs.readFile('Observer.html', function (error,data) {
        res.send(data.toString());
    });
});

var Locations  = require("./models/Locations");

//데이터 조회
app.get('/showdata',function (req, res) {
    console.log('req.params :'  +req.params);
    var name = 'aa';
    Locations.find({name:name}, function (error, data) {
        res.send(data);
    });
});

server.listen(52273, function () {
    console.log('Server running 52273')
});


var io = require('socket.io').listen(server);
io.sockets.on('connection',function (socket) {

    socket.on('join',function (data) {
        socket.join(data);
    });

    socket.on('location',function (data) {


        //데이터 삽입
        Locations.create(data, function(err, data){

            io.sockets.in(data.name).emit('receive',{
                latitude : data.latitude,
                longitude : data.longitude,
                date : Date.now()
            });

        });


        console.log('data1 :'+data.toString('utf8'));
    });
});