const setupJest = {
  roots: ['<rootDir>/src'],
  transform: {
    '^.+\\.tsx?$': 'ts-jest',
    '^.+\\.(css|styl|less|sass|scss|png|jpg|jpeg|ttf|woff|woff2|svg|webp)$': '<rootDir>/imagesTransform.js'
  },
  testRegex: '(/__tests__/.*|(\\.|/)(test|spec))\\.tsx?$',
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  moduleNameMapper: {
    '.+\\.(css|styl|less|sass|scss|ttf|woff|woff2)$': 'identity-obj-proxy',
    'src/(.*)': '<rootDir>/src/$1'
  },
  moduleDirectories: ['node_modules', 'src', 'assets'],
  coveragePathIgnorePatterns: [
    'node_modules',
    'test-config',
    'interfaces',
    'types.ts',
    'jestGlobalMocks.ts',
    '.module.ts',
    '.mock.ts',
    '__snapshots__',
    '.stories.tsx',
    '/utils/development-functions',
    '/pages/development'
  ],
  modulePathIgnorePatterns: ['__tests__/types.ts'],
  collectCoverage: false,
  testEnvironment: 'jest-environment-jsdom'
};

export { setupJest as default };