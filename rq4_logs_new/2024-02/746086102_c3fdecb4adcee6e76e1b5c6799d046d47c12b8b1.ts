import { JestConfigWithTsJest } from 'ts-jest';

const jestConfig: JestConfigWithTsJest = {
  // Automatically clear mock calls and instances between every test
  clearMocks: true,

  coverageDirectory: "coverage",

  // An array of regexp pattern strings used to skip coverage collection
  coveragePathIgnorePatterns: [
    "<rootDir>/node_modules/(?!@foo)"
  ],

  moduleFileExtensions: [
    "ts",
    "tsx",
    "js"
  ],

  // A map from regular expressions to module names that allow to stub out resources with a single module
  moduleNameMapper: {
    "@/(.*)": "<rootDir>/src/$1",
  },

  testEnvironment: "node",

  testRegex: "(/tests/.*|(\\.|/)(test|spec))\\.(jsx?|tsx?)$",

  transform: {
    "^.+\\.(ts|tsx)$": ["ts-jest",{
      "tsconfig": "tsconfig.json",
      "diagnostics": "true"
    }],
  },

  transformIgnorePatterns: [
    "<rootDir>/node_modules/(?!@foo)"
  ]
};

export default jestConfig;