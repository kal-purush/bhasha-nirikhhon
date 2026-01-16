var express = require('express');
var http = require('http');  //build-in module
var app = express();
var errorhandler = require ('errorhandler');

//configure the app
app.set('port', process.argv[2] || process.env.PORT || 9000);
app.set('appName', 'LUX NIRVANA');
app.set('view engine', 'ejs');
app.set('views', __dirname + '/views');

if (process.env.NODE_ENV === 'development') {
	console.log('hi in dev....')
    app.use(errorhandler({
        dumpExceptions: true,
        showStack: true
    }));
} else {
	//in prod
	console.log('in prod::::')
    app.use(errorhandler());
}

//launch/start the server
var server = http.createServer(app).listen(app.get('port'), '0.0.0.0', function() {
    console.log(app.get('appName') + ' server listening on port ' + app.get('port'));
});
server.on('close', function() {
    console.log(app.get('appName') + ' server closed on port ' + app.get('port'));
});

//how to handle incoming requests example get and post requests
app.get('/', function(req, res) {
    //TODO: make async call to database get data and send back in resoponse
    res.render('index', 200);
   

});




// app.use(bodyParser.json());
// app.use(bodyParser.urlencoded({
//   extended: true
// }));
// app.engine('html', consolidate.handlebars);
// app.use(serveStatic(__dirname + '/public'));
// app.set('view engine', 'html');