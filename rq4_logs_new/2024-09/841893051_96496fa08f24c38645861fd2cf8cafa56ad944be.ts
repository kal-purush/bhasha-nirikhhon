import type { Config } from 'jest';
import nextJest from 'next/jest';

import { jestBaseConfig } from './jest.config';

const createJestConfig = nextJest({
	// Provide the path to your Next.js app to load next.config.js and .env files in your test environment
	dir: './',
});

const config: Config = {
	...jestBaseConfig,
	maxWorkers: '50%',
	ci: true,
	verbose: true,
	bail: true,
	testTimeout: 30000,
	reporters: [
		['github-actions', { silent: false }],
		['jest-junit', { outputDirectory: 'reports/junit', outputName: 'js-test-results.xml' }],
		['jest-silent-reporter', { showPaths: true, showWarnings: true, useDots: true }],
		'summary',
	],
};

export default createJestConfig(config);