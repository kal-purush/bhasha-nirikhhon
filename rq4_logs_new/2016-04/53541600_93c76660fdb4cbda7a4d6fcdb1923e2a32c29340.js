'use strict';

var Router = module.exports = function() {
  this.routes = {
    'GET' : {},
    'POST' : {},
  };
};

Router.prototype.get = function(route, cb) {
  this.routes['GET'][route] = cb;
};

Router.prototype.post = function(route, cb) {
  this.routes['POST'][route] = cb;
};

Router.prototype.route = function() {
  return (req, res) => {
    var routeFunction = this.routes[req.method][req.url];
    routeFunction(req, res);
  };
};