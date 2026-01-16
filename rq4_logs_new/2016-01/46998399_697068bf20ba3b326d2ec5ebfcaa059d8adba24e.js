'use strict';

var util = require('util');
var Promise = require('bluebird');
var Repo = require('./repo.js');

/*
 * Constructor. Org represents a github org.
 */
function Org(config) {
  if (this instanceof Org) {
    this.config = config;
    this.client = config.client;
    this.name = config.name;
    this.api = config.client.org(this.name);
  } else {
    return new Org(config);
  }
}

/*
 * Return a list of repo objects for this org.
 */
Org.prototype.repos = function() {
  var self = this;
  return Promise.fromNode(function(cb) {
    self.api.repos(cb);
  })
  .then(function(data) {
    // @todo: validate data[1] result.
    return data[0];
  })
  .map(function(data) {
    return new Repo({
      client: self.api,
      user: self.name,
      repo: data.name
    });
  });
};

/*
 * Export constructor.
 */
module.exports = Org;