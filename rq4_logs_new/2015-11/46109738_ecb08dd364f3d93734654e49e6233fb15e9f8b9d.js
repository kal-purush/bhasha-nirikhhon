/**
 * Created by duongminhkhoa on 13/11/15.
 */
var express = require('express');
var routes 	= require('./routes');
var app = express();

// set up route 
routes.setup(app,__dirname + '/controllers');
app.listen(3000);