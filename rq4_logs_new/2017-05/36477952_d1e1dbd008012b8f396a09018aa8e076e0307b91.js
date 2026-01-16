var main = require('../..');
var Base = require('panthrbase/index');
var chai = require('chai');
var expect = chai.expect;

describe('Implement "missing":', function() {
   it('should return true when the parameter is missing', function() {
      var evs = main.eval('f=function(y) { missing(y) }; f()');

      expect(evs[1].type).to.equal('logical');
      expect(evs[1].value.toArray()).to.deep.equal([true]);
   });
   it('should return false when the parameter is present', function() {
      var evs = main.eval('f=function(y) { missing(y) }; f(4)');

      expect(evs[1].type).to.equal('logical');
      expect(evs[1].value.toArray()).to.deep.equal([false]);
   });
   it('should return false when the parameter has a default', function() {
      var evs = main.eval('f=function(y=4) { missing(y) }; f()');

      expect(evs[1].type).to.equal('logical');
      expect(evs[1].value.toArray()).to.deep.equal([false]);
   });
   it('should error when the name is not a parameter', function() {
      var evs = main.eval('f=function(y) { missing(x) }; f()');

      expect(evs[1].type).to.equal('error');
   });
   it('should not attempt to evaluate its argument', function() {
      var evs = main.eval('f=function(y) { missing(y) }; x=1; f((x=2)); x');

      expect(evs[3].type).to.equal('scalar');
      expect(evs[3].value.toArray()).to.deep.equal([1]);
   });
});