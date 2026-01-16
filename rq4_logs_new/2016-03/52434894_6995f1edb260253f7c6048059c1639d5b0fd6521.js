/*jslint node: true */
'use strict';

var jwt = require('jsonwebtoken');
var User = require('../models/users');

// verify token and attach authorization and user to req.
function isAuthenticated(req, res, next) {
  if(!req.cookies.token) return res.sendStatus(401);

  try {
    req.authorization = jwt.verify(req.cookies.token.replace(/^"(.*)"$/, '$1'), 'somthing crazy');
  }catch(err){
    return res.sendStatus(401);
  }

  if(!req.authorization._id) return res.send(401);

  User.findById(req.authorization._id, function (err, user) {
    if (err) return next(err);
    if (!user) return res.sendStatus(401);
    req.user = user;
    next();
  });
}

//send token on login
function setTokenCookie(req, res) {
  if (!req.user) return res.json(404, { message: 'Something went wrong, please try again.'});
  var token = signToken(req.user._id, req.user.role);
  res.cookie('token', JSON.stringify(token));
  res.redirect('/');
}


function signToken(id) {
  var newToken = jwt.sign({ _id: id }, 'somthing crazy', { expiresInMinutes: 60*5 });
  return newToken;
}


exports.signToken = signToken;
exports.setTokenCookie = setTokenCookie;
exports.isAuthenticated = isAuthenticated;