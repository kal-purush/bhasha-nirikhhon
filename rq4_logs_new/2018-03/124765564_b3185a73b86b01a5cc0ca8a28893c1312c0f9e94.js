//* DEPENDENCIES *//
// Series of npm packages that we will use to give our server useful functionality

var express = require('express')
var bodyParser = require('body-parser')

//* EXPRESS CONFIGURATION *//
// This sets up the basic properties for our express server

// Tells node that we are creating an 'express' server
var app = express()

// Set and inital port that will be used later in our listener.
var PORT = process.env.PORT || 8080

// Sets up the Express app to handle data parsing
app.useapp.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json())

//* ROUTER *//
// The below points our server to a series of "route" files.
// These routes give our server a "map" of how to respond when users visit or request data from various URLs.

require("./routing/apiRoutes")(app);
require("./routing/htmlRoutes")(app);