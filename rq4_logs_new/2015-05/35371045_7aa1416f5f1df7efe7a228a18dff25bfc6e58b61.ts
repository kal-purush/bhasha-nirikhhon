var expect = require('chai').expect;
import Operation = require('../../app/ts/operations/operation');
import AddOperation = require('../../app/ts/operations/add-operation');

describe('AddOperation', function() {
  var op: AddOperation;

  before(function() {
    op = new AddOperation(2, 3);
    op.position = 4;
    op.value = 'a';
  });

  it('increments the position with an add operation having less position', function() {
    var op2 = new Operation(3, 3, 'add');
    op2.position = 2;
    op2.value = 'b';

    var t = op.transformWithAdd(op2);
    expect(t.position).be.equal(op.position + 1);
  });

  it('increments the position with an add operation having equal position and less site id', function() {
    var op2 = new Operation(1, 3, 'add');
    op2.position = 4;
    op2.value = 'b';

    var t = op.transformWithAdd(op2);
    expect(t.position).be.equal(op.position + 1);
  });

  it('does not transform with an add operation having equal position and more site id', function() {
    var op2 = new Operation(5, 3, 'add');
    op2.position = 4;
    op2.value = 'b';

    var t = op.transformWithAdd(op2);
    expect(t.position).be.equal(op.position);
  });

  it('does not transform with an add operation having more position', function() {
    var op2 = new Operation(3, 3, 'add');
    op2.position = 5;
    op2.value = 'b';

    var t = op.transformWithAdd(op2);
    expect(t.position).be.equal(op.position);
  });

  it('does not transform with any update operations', function() {
    var op2 = new Operation(1, 3, 'update');
    op2.position = 1;

    var t = op.transformWithUpdate(op2);
    expect(t.position).be.equal(op.position);
  });

  it('decrements the position if that is more than the position of the corresponding remove operation', function() {
    var op2 = new Operation(3, 3, 'remove');
    op2.position = 1;

    var t = op.transformWithRemove(op2);
    expect(t.position).be.equal(op.position - 1);
  });

  it('does not transform if the position is less than or equal to the position of the corresponding remove operation', function() {
    var op2 = new Operation(3, 3, 'remove');
    op2.position = 4;

    var t = op.transformWithRemove(op2);
    expect(t.position).be.equal(op.position);
  });
});