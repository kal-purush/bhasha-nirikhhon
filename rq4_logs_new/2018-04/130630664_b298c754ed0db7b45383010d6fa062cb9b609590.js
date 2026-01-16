module.exports = {
  setupTestFrameworkScriptFile: "<rootDir>setupTests.js",
  coverageDirectory: "coverage",
  coveragePathIgnorePatterns: ["/node_modules/", "<rootDir>/setupTests.js"],
  coverageReporters: ["json", "lcov", "text"],
  coverageThreshold: {
    global: {
      branches: 90,
      functions: 90,
      lines: 90,
      statements: -10
    }
  },
  testEnvironment: "jsdom"
};