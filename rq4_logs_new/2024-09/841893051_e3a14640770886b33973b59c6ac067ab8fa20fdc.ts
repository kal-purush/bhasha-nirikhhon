import type { Config } from 'jest';
import nextJest from 'next/jest';

const createJestConfig = nextJest({
	// Provide the path to your Next.js app to load next.config.js and .env files in your test environment
	dir: './',
});

const config: Config = {
	// Automatically clear mock calls, instances, contexts and results before every test
	clearMocks: true,
	// Indicates whether the coverage information should be collected while executing the test
	collectCoverage: true,
	// The directory where Jest should output its coverage files
	coverageDirectory: 'coverage',
	// Indicates which provider should be used to instrument code for coverage
	coverageProvider: 'v8',
	// A list of paths to modules that run some code to configure or set up the testing framework before each test
	setupFilesAfterEnv: ['<rootDir>/src/testing/jest.setup.ts'],
	// The test environment that will be used for testing
	testEnvironment: 'jsdom',
	// A map from regular expressions to module names or to arrays of module names that allow to stub out resources with a single module
	moduleNameMapper: {
		'^@/components/(.*)$': '<rootDir>/src/components/$1',
	},
};

export default createJestConfig(config);