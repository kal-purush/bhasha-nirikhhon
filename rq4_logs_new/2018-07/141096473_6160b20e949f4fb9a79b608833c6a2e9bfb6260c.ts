import 'mocha';
import { assert, expect } from 'chai';
import { test } from '../lib/index';

describe('index', function () {
    describe('test', function () {
        it('should return 42', function (done) {
            const result = test();

            expect(result).to.equal(42);
        });
    });
});