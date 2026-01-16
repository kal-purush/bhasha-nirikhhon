'use strict';
var Promise = require('bluebird')
var assert = require('assert')
var Joi = require('joi')
var Hoek = require('hoek')
var Boom = require('boom')

var internals = {}

var routerSchema = Joi.object().keys({
  path: Joi.string().required(),
  method: Joi.string().required(),
  handler: Joi.any().required()
});

module.exports = exports = internals.Router = function (options) {

  if (!(this instanceof internals.Router))
    return new internals.Router();

  this.routes = {};
  var opts = options || {};
  return this;
}


internals.Router.prototype.register = function (route) {
  var routes = this.routes;

  Hoek.assert(route, 'route is required');
  var result = Joi.validate(route, routerSchema);
  if (result.error) {
    console.log('error = ' + result.error)
    throw (result.error)
  }

  console.log('routes = ' + this.routes);

  if (!routes[route.path])
    routes[route.path] = {}
  routes[route.path][route.method] = route;
  if (route.initialize && typeof route.initialize == 'function') {
    route.validate(event.context)
  }
}

internals.Router.prototype.handler = function (event, context) {
  var request = event;
  var response = {};
  if (event.context && event.context.path) {
    let route = this.routes[event.context.path][event.context.method];
    if (route) {
      route.handler(event, context, response)
        .then((response) => {
          context.done(null, response);
        })
        .catch((error) => {
          context.fail(error, response);
        })
    } else {
      context.fail(Boom.notImplemented('Handler not implemented'))
    }
  } else {
    context.fail(Boom.badImplementation('Request context, method, path are required'))
  }
}