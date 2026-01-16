module.exports = {
    preset: 'jest-preset-angular',
    setupFilesAfterEnv: ['<rootDir>/setup-jest.ts'],
    testPathIgnorePatterns: ['<rootDir>/node_modules/', '<rootDir>/dist/'],
    testMatch: ['**/+(*.)+(spec).+(ts)'],
    collectCoverage: true,
    coverageReporters: ['html'],
    coverageDirectory: 'coverage/my-app',
};