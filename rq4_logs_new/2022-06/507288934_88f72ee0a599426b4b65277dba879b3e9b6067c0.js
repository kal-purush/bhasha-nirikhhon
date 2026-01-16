const { describe, it } = require('mocha');
const { expect } = require('chai');
const vars = require('../variables');

describe('Task 2 - Variables and Data Types', () => {

  it('int variable should be created', () => {
    expect(vars.int).to.equal(1);
  });

  it('int2 variable should be created', () => {
    expect(vars.int2).to.equal(2);
  });

  it('str variable should be created', () => {
    expect(vars.str).to.equal('John');
  });

  it('str2 variable should be created', () => {
    expect(vars.str2).to.equal('John Dou');
  });

  it('bool variable should be created', () => {
    expect(vars.bool).to.equal(false);
  });

  it('arr variable should be created', () => {
    const expected = [1,2,3,4,5];

    expect(Array.isArray(vars.arr)).to.equal(true);
    vars.arr.forEach(el => {
      expect(expected.includes(el), 'arr variable is not equal to [1,2,3,4,5]').to.equal(true);
    });
  });

  it('fifth variable should be created', () => {
    expect(vars.arr[4]).to.equal(vars.fifth);
  });

  it('human variable should be created', () => {
    expect(typeof vars.human).to.equal('object');
    expect(vars.human.firstName).to.equal('John');
    expect(vars.human.lastName).to.equal('Dou');
    expect(vars.human.fullName).to.equal('John Dou');
  });

});