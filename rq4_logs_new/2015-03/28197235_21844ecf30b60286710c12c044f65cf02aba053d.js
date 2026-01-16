exports.lab = require('lab-bdd')(require('lab'));
var path = require('path');
var cwd = process.cwd();
var loadConfig = require('confi');
var Rapptor = require('../');

describe('Rapptor#all', function() {

  var rapptor = rapptor = new Rapptor({
      cwd: __dirname
    });

  before(function(done) {
    rapptor.start(function(err, server) {
      // expect(err).to.equal(null);
      done();
    });
  });

  it('should use the all helper correctly', function(done) {
    
    var server = rapptor.server;
    server.inject({
      method: 'GET',
      url: "/all-test"
    }, function(response) {
      expect(response.statusCode).to.equal(200);
      //console.log(response.result);
      var docBody = response.result;
      docBody = docBody.replace(/\n|\s{2,}/g, "");

      expect(docBody).to.equal("<h1>Hi!</h1><p>true</p>");
      done();
    });
    
  });

  after(function(done) {
    rapptor.stop(function() {
      done();
    });
  });
});