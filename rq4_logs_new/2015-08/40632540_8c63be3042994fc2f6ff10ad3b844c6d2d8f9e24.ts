/// <reference path="../typings/tsd.d.ts" />

import * as NervousSigmoid from '../lib/sigmoid';
import chai = require('chai');

var expect = chai.expect;

describe ('nervous-array', () => {
  
  it ('should calculate some value with the sigmoid function', () => {
    expect(NervousSigmoid.sigmoid(0)).to.equal(0.5);
    expect(NervousSigmoid.sigmoid(Infinity)).to.equal(1);
    expect(NervousSigmoid.sigmoid(-Infinity)).to.equal(0);
  });
  
  it ('should calculate some value with the sigmoid prime function', () => {
    expect(NervousSigmoid.sigmoidPrime(0)).to.equal(0.25);
    expect(NervousSigmoid.sigmoidPrime(Infinity)).to.equal(0);
  });
  
});