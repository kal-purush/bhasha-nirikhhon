import { expect } from 'chai';
import * as random from './utils/random';
import { HoukBus } from '..';
import 'mocha';

const randomString = random.randomString();
const randomObject = random.randomObject();

describe('basic hookchain with bus', () => {
	it('should run in order', async () => {
		const test = new HoukBus();
		let thisArg;
		let arg;

		test.on('test', function(string: string) {
			thisArg = this;
			arg = string;

			return string + string;
		});

		test.on('test', async function(string: string) {
			await new Promise(resolve => setTimeout(resolve, 5));
			return string.toUpperCase();
		});

		const value = await test.emit('test', randomObject, randomString);

		expect(thisArg).to.eql(randomObject);
		expect(arg).to.equal(randomString);
		expect(value).to.equal((randomString + randomString).toUpperCase());
	});
});