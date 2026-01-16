var algorithmia = require("algorithmia");
var fs = require("fs");
var Twitter = require('twitter');
var AWS = require('aws-sdk');

//Load config secrets etc (for development)
require('dotenv').config();

var twitterClient = new Twitter({
  consumer_key: process.env.TWITTER_CONSUMER_KEY,
  consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
  access_token_key: process.env.TWITTER_ACCESS_TOKEN,
  access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET
});
var s3bucket = new AWS.S3({params: {Bucket: 'unjpeg'}});
var algorithmiaClient = algorithmia(process.env.ALGORITHMIA_API_KEY).algo("algo://chinery/unjpeg/16b50586fa8c67bc09b5a4d6ae43494cb75a920d");

function fixImageAndTweet(imageUrl, replyToTweetId, replyToUsername){
  //Send the image to algorithmia for the artefacts to be removed.
  algorithmiaClient
    .pipe(imageUrl)
    .then(function(response) {
      console.log('algorithmia replied!');
      var data = response.get(); //buffer object.
      //Upload the cleaned image to S3.
      var filename = Math.random()+".png";
      var params = {Key: filename, Body: data, ContentType: "image/png", ACL: "public-read"};
      s3bucket.upload(params, function(err, data2) {
        if (err) {
          console.log("Error uploading data: ", err);
        } else {
          //Send reply to the tweeter with a link to the UNJPEG'd image.
          var newTweet = {status: "@"+replyToUsername+" Here's your image, cleaned and UnJPEG'd: https://s3.amazonaws.com/unjpeg/"+filename,
            in_reply_to_status_id: replyToTweetId
          };
          twitterClient.post('statuses/update', newTweet, function(error, tweet, reponse){
            if( error ){
              console.error(error);
            }
            else{
              console.log(tweet);
            }
          })
        }
      });
    });
  console.log('sent to algorithmia');
}

exports.handler = function (request) {
  //Check mentions from twitter.
  var params = {screen_name: 'nodejs'};
  twitterClient.get('statuses/mentions_timeline', params, function(error, tweets, response) {
    if (error) {
      console.error(error);
    }
    else{
      tweets.forEach(function(tweet){
        if( true ) //TODO: Test if we already saw this tweet and replied.
        {
          if( tweet.quoted_status && tweet.quoted_status.entities.media ){ //Eg. user referenced the tweet but sent it to @unjpeg (majority use case)
            tweet.quoted_status.entities.media.forEach(function(entity){
              if( entity.type == 'photo' && entity.media_url_https.endsWith('jpg') ){
                fixImageAndTweet(entity.media_url_https, tweet.id_str, tweet.user.screen_name);
              }
            });
          }
        }
      })

    }
  });


};

//For testing locally, when running "node app.js" from CLI.
exports.handler();