import assert from 'power-assert';
import { Observable } from 'rxjs';
import pairwise from '../src/pairwise';

describe('pairwise', function() {
  context('when []', function() {
    it('works', function() {
      const source = Observable.fromArray([]);
      pairwise(source)
        .toArray()
        .subscribe(array => {
          assert(array.length === 0);
        });
    });
  });

  context('when [1]', function() {
    it('works', function() {
      const source = Observable.fromArray([1]);
      pairwise(source)
        .toArray()
        .subscribe(array => {
          assert(array.length === 0);
        });
    });
  });

  context('when [1, 2, 3, 4]', function() {
    it('works', function() {
      const source = Observable.fromArray([1, 2, 3, 4]);
      pairwise(source)
        .toArray()
        .subscribe(array => {
          assert(array.length === 3);
          const [i1, i2, i3] = array;
          assert.deepEqual(i1, [1, 2]);
          assert.deepEqual(i2, [2, 3]);
          assert.deepEqual(i3, [3, 4]);
        });
    });
  });
});