var express = require('express');
var router = express.Router();
var requests = require('request');
var User = require('./../models/user');

router.get('/', function(request, response) {
  request.user = request.session.facebook
  var prodRequests = []

  let options = {
    method: 'GET',
    url: 'http://pablee-api:8000/api/v1/requests',
    headers: {
      'content-type': 'application/json'
    },
    json: true
  }

  requests(options, function(err, resp, body) {
    for (index in body) {
      prodRequest = body[index];

      User.findOne({uuid: prodRequest.uuid}).then(function(user) {
        prodRequest.user = user;
        prodRequests.push(prodRequest);
      });
    }

    setTimeout(function() {
      return response.render('travel', {user: request.user, prodRequests: prodRequests });
    }, 1000)
     
  });
});

module.exports = router;