import {expect} from 'chai';

import {Token} from './tokenizer/token';
import {Compound} from './reducer/compound';
import {Reducer} from './reducer';

describe('@bmoor/compiler', function () {
	describe('Reducer', function () {
		class EinsToken extends Token {
			toExpressable() {
				return [];
			}
		}

		class ZweiToken extends Token {
			toExpressable() {
				return [];
			}
		}

		class DreiToken extends Token {
			toExpressable() {
				return [];
			}
		}

		class FierToken extends Token {
			toExpressable() {
				return [];
			}
		}

		const eins = new EinsToken('1');
		const otherEins = new EinsToken('2');
		const zwei = new ZweiToken('3');
		const drei = new DreiToken('4');
		const fier = new FierToken('5');

		class Compound1 extends Compound {
			static pieces = [EinsToken, ZweiToken];

			toExpressable() {
				return [];
			}
		}

		class Compound2 extends Compound {
			static pieces = [EinsToken, ZweiToken, DreiToken];

			toExpressable() {
				return [];
			}
		}

		class Compound3 extends Compound {
			static pieces = [EinsToken, ZweiToken, FierToken];

			toExpressable() {
				return [];
			}
		}

		it('properly reduce a whole set', async function () {
			const reducer = new Reducer([Compound1, Compound2, Compound3]);

			const tokens = reducer.reduce([eins, zwei]);

			expect(tokens.map((token) => token.getReference())).to.deep.equal([
				'{:}',
				'|:|',
				'{:}'
			]);
		});

		it('properly do something', async function () {
			const reducer = new Reducer([Compound1, Compound2, Compound3]);

			const tokens = reducer.reduce([eins, zwei, otherEins, drei, fier]);

			expect(tokens.map((token) => token.getReference())).to.deep.equal([
				'{:}',
				'|:|',
				'{:}'
			]);
		});
	});
});