import { describe, it } from '@xutl/test';
import { strict as assert } from 'assert';

import { parse, string } from '../args';

const args = [process.argv0, process.argv[1] as string];
describe('args', () => {
	describe('string', () => {
		it('single', () => {
			const parsed = parse([...args, '--str', 'hello'], string('str'));
			assert.deepStrictEqual(parsed.params, {
				str: 'hello',
			});
		});
		it('multi', () => {
			//only the last one is taken
			const parsed = parse([...args, '--str', 'hello', '--str', 'world'], string('str'));
			assert.deepStrictEqual(parsed.params, {
				str: 'world',
			});
		});
		it('multi-seperated', () => {
			const parsed = parse([...args, '--s1', 'hello', '--s2', 'abc', '--s1', 'world'], string('s1'), string('s2'));
			assert.deepStrictEqual(parsed.params, {
				s1: 'world',
				s2: 'abc',
			});
		});
	});
});