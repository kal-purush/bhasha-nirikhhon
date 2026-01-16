/// <reference path="../typings/tsd.d.ts" />
/// <reference path="./custom-typings/botkit.d.ts" />

import Botkit = require('botkit');
import request = require('request');
import fs = require('fs');
import nconf = require('nconf');

nconf.file({ file: './config.json'});

var ONE_BUS_AWAY_KEY = nconf.get('ONE_BUS_AWAY');
var SLACK_TOKEN = nconf.get('SLACK_TOKEN');
var interval;

var controller = Botkit.slackbot({
 debug: true
});

controller.spawn({
    token: SLACK_TOKEN
}).startRTM(function(err) {
    if (err) {
        throw new Error(err);
    }
});

controller.hears(['nvm'],['direct_message'],function(bot, message) {
    if (interval) {
        clearInterval(interval);
    }
});

controller.hears(['bus'],['direct_message'],function(bot, message) {
    bot.reply(message, 'Let me check...');
    var oneBusAwayUrl = 'http://api.pugetsound.onebusaway.org/api/where/arrivals-and-departures-for-stop/' 
        + '1_71334.json?key=' 
        + ONE_BUS_AWAY_KEY;
        
    sayNextArrival(message, bot, oneBusAwayUrl);
    interval = setInterval(sayNextArrival.bind(this, message, bot, oneBusAwayUrl), 5000);
});

var sayNextArrival = function(message, bot, url) {
    request(url, function (error, response, body) {
        if (!error && response.statusCode == 200) {
            var stopInfo = JSON.parse(body);
            var nextBus = stopInfo.data.entry.arrivalsAndDepartures[0];
            if (nextBus) {
                var nextArrivalTime = new Date(nextBus.predictedArrivalTime);
                bot.say({ 
                    text: nextBus.routeShortName + ': ' + nextArrivalTime.toTimeString(),
                    channel: message.channel
                });    
            } else {
                bot.say({ 
                    text: 'No arrivals in the next 35 min',
                    channel: message.channel
                });
            }
            
        }
    });
}