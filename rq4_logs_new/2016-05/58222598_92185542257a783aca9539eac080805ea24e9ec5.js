'use strict';

let _ = require('lodash');

describe('_.lowerCase()', () => {
   it('should be in lower case.', () => {
       let msg = _.lowerCase('Hello World');
       msg.should.be.equal('hello world');
   });
});