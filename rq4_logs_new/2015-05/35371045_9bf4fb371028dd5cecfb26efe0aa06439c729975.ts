var expect = require('chai').expect;
import Operation = require('../../app/ts/operations/operation');

describe('Operation', function() {
  var op;

  before(function() {
    op = new Operation(1, 2, 'add');
    op.position = 3;
    op.value = 'a';
  });

  it('clones oneself', function() {
    var clone = op.clone();
    expect(clone).not.equal(op);
    expect(clone.siteId).be.equal(op.siteId);
    expect(clone.seqId).be.equal(op.seqId);
    expect(clone.position).be.equal(op.position);
    expect(clone.value).be.equal(op.value);
  });
});