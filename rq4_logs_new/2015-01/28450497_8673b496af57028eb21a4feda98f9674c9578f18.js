'use strict';

var _ = require('lodash');
var Joi = require('joi');
var mongoose = require('mongoose'),
  Resztvevo = mongoose.model('Resztvevo');

function egyResztvevo(req, reply) {
  Resztvevo.findById(req.params.resztvevoId)
    .populate('jogas alkalom')
    .exec(function (err, resztvevo) {
    if (err) {
      reply({errors: err.errors});
    } else {
      reply(resztvevo);
    }
  });
}

function updateResztvevo(req, reply) {
  var resztvevo = req.pre.resztvevo;
  resztvevo = _.extend(resztvevo, req.payload);
  resztvevo.save(function (err, resztvevo) {
    if(err) {
      reply(err);
    } else {
      reply(resztvevo);
    }
  });
}

module.exports = function (prefix) {
  return [
    {
      method: 'GET',
      path: prefix + '/{resztvevoId}',
      config: {
        cors: true,
        handler: egyResztvevo
      }
    },
    {
      method: 'POST',
      path: prefix + '/{resztvevoId}/update',
      config: {
        cors: true,
        handler: updateResztvevo,
        pre: [
          {
            method: egyResztvevo,
            assign: 'resztvevo'
          }
        ]
      }
    }
  ];
};