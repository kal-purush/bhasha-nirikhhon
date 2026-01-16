/*
 * For a detailed explanation regarding each configuration property and type check, visit:
 * https://jestjs.io/docs/configuration
 */

export default {
  globals: {
    'ts-jest': {
      'useESM': true
    }
  },
  transform: {
    "^.+\\.(ts|tsx)$": "ts-jest"
  },  
  extensionsToTreatAsEsm: [".ts"],
  preset: 'ts-jest/presets/default-esm',
  coverageDirectory: 'coverage',
  coverageProvider: 'v8',
};