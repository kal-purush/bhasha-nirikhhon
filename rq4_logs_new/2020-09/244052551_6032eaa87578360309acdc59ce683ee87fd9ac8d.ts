import { expect } from 'chai';
import { count, generate } from '../src/utils/util';

describe('util#generate()', function () {
  it('generate: should generate the expected length, in bytes, of a random string', function () {
    expect(generate(128).length).to.equal(128)
    expect(generate(256).length).to.equal(256)
    expect(generate(512).length).to.equal(512)
    expect(generate(1024).length).to.equal(1024)
    expect(generate(4096).length).to.equal(4096)
  })
})

describe('util#count()', function () {
  it('count: should return the correct byte length of the provided string', function () {
    expect(count(generate(16))).to.equal(16)
    expect(count(generate(128))).to.equal(128)
    expect(count(generate(256))).to.equal(256)
  })
})