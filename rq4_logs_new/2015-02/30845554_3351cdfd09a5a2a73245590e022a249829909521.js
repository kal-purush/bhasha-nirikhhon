var settings = require('./settings');
var express = require('express');
var bodyParser = require('body-parser');
var Twit = require('twit');
var winston = require('winston');


var app = express();
app.use(bodyParser.json());

app.use(bodyParser.urlencoded({ extended: false }));

// Set up twitter
var twitterClient = new Twit(settings.twitterCredentials);

app.post('/', function(req, res){
    var body = req.body;
    var message = {
        display_coordinates: true,
        lat: body.location.x,
        long: body.location.y,
        status: body.message.text.substring(0, 140)
    };

    twitterClient.post('statuses/update', message, function(err, data, response) {
        if (err) {
            res.status(400).json(err);
        } else {
            // data.id = post response id from twitter
            var payload = {
                id: data.id
            };
            res.status(201).json(payload);
        }
    });

    winston.log('info', 'Hello distributed log files!');
});

app.listen(8999);

winston.add(winston.transports.File, { filename: settings.loggerFile });