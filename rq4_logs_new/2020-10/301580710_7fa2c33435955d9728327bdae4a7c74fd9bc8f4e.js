module.exports = {
  extends: ['eslint:recommended', 'airbnb-base'],
  parser: '@babel/eslint-parser',
  parserOptions: {
    ecmaVersion: 12,
  },
  env: {
    browser: true,
    node: true,
    es2020: true,
  },
  plugins: ['html'],
  rules: {
    quotes: ['error', 'single', { avoidEscape: true }],
    'linebreak-style': 'off',
    'no-unused-vars': 'warn',
    indent: [
      'error',
      2,
      {
        SwitchCase: 1,
      },
    ],
    'no-param-reassign': ['warn', { props: false }],
    'no-restricted-globals': 'warn',
    'arrow-body-style': 'warn',
    'no-plusplus': 'off',
    'no-multiple-empty-lines': ['error', { max: 2 }],
    strict: 'warn',
    'import/prefer-default-export': 'warn',
    'no-restricted-syntax': 'warn',
    'space-before-function-paren': 'warn',
    'arrow-parens': ['error', 'always'],
  },
};