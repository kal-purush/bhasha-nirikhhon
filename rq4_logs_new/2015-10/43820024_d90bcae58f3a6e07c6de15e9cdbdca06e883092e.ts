///<reference path="typings/express/express.d.ts" />
///<reference path="typings/node/node.d.ts"/>
///<reference path="typings/body-parser/body-parser.d.ts"/>
///<reference path="typings/errorhandler/errorhandler.d.ts"/>

import http = require('http');
import express = require('express');
import path = require('path');
import bodyParser = require('body-parser');
import errorhandler = require('errorhandler');

var app = express();


app.set('port', process.env.port||3000);
app.set('views', __dirname+'../views');
app.set('view engine', 'jade');


app.use(bodyParser.json());
app.use(express.static(__dirname+'public'));

app.use(errorhandler());

import ctrl = require('./controllers/IController');
ctrl.Controllers.Controller.getAllRoutes(path.join(__dirname, 'controllers')).forEach(rt=>{
	app.use(rt.Name, rt.Router);
})

// import rtHome = require('./controllers/HomeController');

// app.use('/', new rtHome.Controllers.HomeController().registerRoute());

http.createServer(app).listen(app.get('port'), function(){
	console.log('server starting');
});

// expresss./