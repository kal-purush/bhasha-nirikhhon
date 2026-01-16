import * as smoke from './smoke';

smoke.suite('Smoke → Ignore', [
	{
		pattern: 'fixtures/**/*',
		globOptions: { ignore: ['**/*.md'] },
		fgOptions: { ignore: ['**/*.md'] }
	},
	{
		pattern: 'fixtures/**/*',
		globOptions: { ignore: ['**/*.md'] },
		fgOptions: { ignore: ['!**/*.md'] }
	}
]);