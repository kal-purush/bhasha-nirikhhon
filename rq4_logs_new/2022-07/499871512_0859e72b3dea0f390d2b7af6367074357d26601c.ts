import {expect} from 'chai';

import {parser} from './parser';

describe('@bmoor/path', function () {
	describe('parser', function () {
		it('should work with dots', function () {
			const fn = parser.compile('foo.bar');

			expect(fn({foo: {bar:'ok'}})).to.equal('ok');
		});

		it('should work with brackets', function () {
			const fn = parser.compile('["foo"]["bar"]');

			expect(fn({foo: {bar:'ok'}})).to.equal('ok');
		});

		it('should work with mixed cases', function () {
			const fn = parser.compile('foo["bar"].value');

			expect(fn({foo: {bar:{value:'ok'}}})).to.equal('ok');
		});
	});
});