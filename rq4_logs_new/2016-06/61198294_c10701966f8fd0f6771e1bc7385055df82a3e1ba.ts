'use strict';

const twitter = require('twitter');
const config = require('config');

var client = new twitter(config.get('twitter_keys'));

exports = module.exports = function (app) {
    app.get('/api/twitter/', (req, res) => {

        client.get('statuses/home_timeline', function (error, tweets, response) {
            if (error) throw error;
            console.log(tweets);  // The favorites.
            //console.log(response);  // Raw response object.
            res.jsonp(tweets);
        });
    });
};