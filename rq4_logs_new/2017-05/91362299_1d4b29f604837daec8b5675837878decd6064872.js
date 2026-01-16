module.exports = {
  extends: [
    './rules/best-practices',
    './rules/browser',
    './rules/errors',
    './rules/es6',
    './rules/flowtype',
    './rules/imports',
    './rules/jsx-ally',
    './rules/mocha',
    './rules/node',
    './rules/react',
    './rules/redux-saga',
    './rules/strict',
    './rules/style',
    './rules/variables',
  ].map(require.resolve),
  parserOptions: {
    ecmaVersion: 2017,
    sourceType: 'module',
    ecmaFeatures: {
      experimentalObjectRestSpread: true
    },
  },
  parser: 'babel-eslint',
  rules: {
    strict: 'error',
  }
};