const jestConfig = {
    transform: {
        '^.+\\.ts?$': 'esbuild-jest',
    },
    clearMocks: true,
    collectCoverage: true,
    coverageDirectory: 'coverage',
    coverageProvider: 'v8',
    testMatch: ['**/tests/**/*.test.ts'],
    moduleNameMapper: {
        '@/(.*)': '<rootDir>/src/$1',
    },
};

export default jestConfig;