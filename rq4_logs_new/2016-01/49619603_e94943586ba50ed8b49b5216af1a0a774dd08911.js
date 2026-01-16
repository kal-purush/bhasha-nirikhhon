var expect = require('chai').expect;
var mongoose = require('mongoose');
var User = require('../models/user.js');
var Pin = require('../models/pin.js');

describe('Define Models', function(){

  describe('User Model', function () {

    it('User should be a Mongoose model', function () {
      expect(new User()).to.be.instanceOf(mongoose.Model);
    });
    it('should have a schema', function () {
      expect(User.schema).to.exist;
    });

  });

  describe('Pin Model', function () {

    it('pin should be a Mongoose model', function () {
      expect(new Pin()).to.be.instanceOf(mongoose.Model);
    });
    it('should have a schema', function () {
      expect(Pin.schema).to.exist;
    });

  });

})