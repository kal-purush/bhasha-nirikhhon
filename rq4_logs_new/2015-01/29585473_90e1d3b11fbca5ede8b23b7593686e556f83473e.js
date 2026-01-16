var express = require('express');
// Use Parse.Cloud.define to define as many cloud functions as you want.
// For example:
Parse.Cloud.define("hello", function(request, response) {
  response.success("Hello world!");
});
var app = express();
 
// Global app configuration section
app.use(express.bodyParser());  // Populate req.body
app.post('/notify_message',
         express.basicAuth('YOUR_USERNAME', 'YOUR_PASSWORD'),
         function(req, res) {
  // Use Parse JavaScript SDK to create a new message and save it.
  var Message = Parse.Object.extend("Message");
  var message = new Message();
  message.save({ text: req.body.text }).then(function(message) {
    res.send('Success');
  }, function(error) {
    res.status(500);
    res.send('Error');
  });
});
 

app.post('https://hooks.slack.com/services/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', express.bodyParser(), function (req, res) {
  //req.body is your array of objects now:
  // [{id:134123, url:'www.qwer.com'},{id:131211,url:'www.asdf.com'}]
});
app.listen();