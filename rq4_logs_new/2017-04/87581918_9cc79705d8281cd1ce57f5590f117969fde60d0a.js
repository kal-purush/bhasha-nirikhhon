 // var http = require('http');
 // http.createServer(function (req,res) {
 //     res.writeHead(200,{'Content-Type':'text/plain'});
 //     res.end("Hello World");
 // }).listen(1337,"127.0.0.1");
 // console.log("Server running at http://127.0.0.1:1337/");
// ----------------------------------------------------------------------------------
//var net = require('net');

//var server = net.createServer(function(socket){
//    socket.write("Echo Server:\r\n");
//    socket.pipe(socket);
//});

//server.listen(1337,"127.0.0.1");
//----------------------------------------------------------------------------------
//  var app = require('express')()
//  var bodyParser = require('body-parser')
//  var server = require('http').createServer(app)
//  var net = require('net');
//
//  var sockets = [];
//  /*
//   * Callback method executed when data is received from a socket
//   */
//  function receiveData(data) {
//      for(var i = 0; i<sockets.length; i++) {
//          sockets[i].write(data);
//      }
//  }
//
//  // app.get('/', function(req, res) {
//  //     console.log("connet");
//  //     res.send('Hello world!');
//  //     //sockets[0].write("Client send Request");
//  // })
//  /*
//   * Callback method executed when a new TCP socket is opened.
//   */
//  function newSocket(socket) {
//      sockets.push(socket);
//      socket.write('Welcome to the Telnet server!\n');
//      socket.on('data', function(data) {
//
//      })
//  }
//
//  // Create a new server and provide a callback for when a connection occurs
//  var server = net.createServer(newSocket);
//
//  // Listen on port 3000
//  server.listen(3000);
 //----------------------------------------------------------------------------------
 // var net = require('net');
 //
 // var server = net.createServer(function (socket) {
 //     socket.write('Welcome to the Telnet server!');
 // }).listen(8888);
 //----------------------------------------------------------------------------------
 var express = require('express');
 var http = require('http');
 var path = require('path');

 var app = express();

 // all environments
 app.set('port', process.env.PORT || 3000);
 app.set('views', __dirname + '/views');
 app.set('view engine', 'jade');
 app.use(express.favicon());
 app.use(express.logger('dev'));
 app.use(express.bodyParser());
 app.use(express.methodOverride());
 app.use(app.router);
 app.use(express.static(path.join(__dirname, 'public')));

 var net = require('net');

 var sockets = [];
 var mainsocket;

 var humidity = null;
 var soil_moisture= null;
 var temprture = null;


 function receiveData(data) {
     // console.log(String.fromCharCode(data[0]));
     // console.log(data.length);
     // console.log(data);
     try {
         var sens = "";
         for (var i = 0; i < data.length; i++) {
             sens += String.fromCharCode(data[i]);
         }
         console.log(sens);
         sens = sens.split(" ");
         var stringArray = new Array();
         for (var i = 0; i < sens.length; i++) {
             stringArray.push(sens[i]);
             if (i != sens.length - 1) {
                 stringArray.push(" ");
             }
         }
         temprture = sens[0];
         soil_moisture = sens[1];
         humidity = sens[2];
         mainsocket.write("OK");
         //console.log(getDateTime());
     }
     catch (ex){
         console.log(ex);
     }
     // for(var i = 0; i<sockets.length; i++) {
     //     sockets[i].write(data);
     // }
 }
 //------------------------------------------------------
 //getCurrent Time
 //------------------------------------------------------
 function getDateTime() {

     var date = new Date();

     var hour = date.getHours();
     hour = (hour < 10 ? "0" : "") + hour;

     var min  = date.getMinutes();
     min = (min < 10 ? "0" : "") + min;

     var sec  = date.getSeconds();
     sec = (sec < 10 ? "0" : "") + sec;

     var year = date.getFullYear();

     var month = date.getMonth() + 1;
     month = (month < 10 ? "0" : "") + month;

     var day  = date.getDate();
     day = (day < 10 ? "0" : "") + day;

     return year + "/" + month + "/" + day + "     " + hour + ":" + min + ":" + sec;

 }
 //-------------------------------------------------------
 function closeSocket(socket) {
     var i = sockets.indexOf(socket);
     if (i != -1) {
         sockets.splice(i, 1);
     }
 }
//---------------------------------------------------------
 app.get('/on', function(req, res){
     mainsocket.write("on");
     res.json({Motor:"ON"});
 });

 app.get('/off', function(req, res){
     mainsocket.write("off");
     res.json({Motor:"OFF"});
 });

 app.get('/a', function(req, res){
     mainsocket.write("a");
     res.json({name:"hamid"});
 });

 app.get('/b', function(req, res){
     mainsocket.write("send to client\n");
     res.json({
         Temperature:temprture,
         Soil_moisture:soil_moisture,
         Humidity:humidity
     });
 });
 // app.put('/flight/:number/arrived', routes.arrived);
 //-------------------------------------------------------

 function newSocket(socket) {
     mainsocket = socket;
     sockets.push(socket);
     socket.write('Welcome to the Telnet server!\n');
     socket.on('data', function(data) {
         receiveData(data);
     })
     socket.on('end', function() {
         closeSocket(socket);
     })
     socket.on('error', function(err) {
         console.log(err);
         console.log("\n Error \n");
     });
 }

 // Create a new server and provide a callback for when a connection occurs
 var server = net.createServer(newSocket);

 // Listen on port 2000
 server.listen(2000,function () {
     console.log('Tellnet server listening on port 2000');
 });

 http.createServer(app).listen(app.get('port'), function(){
     console.log('Express server listening on port ' + app.get('port'));
 });
 //----------------------------------------------------------------------------------