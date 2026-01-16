module.exports = {
    env: {
      commonjs: true,
      es6: true,
      node: true,
      jest: true,
    },
    extends: ['airbnb-base', 'prettier'],
    plugins: ['prettier'],
    globals: {
      Atomics: 'readonly',
      SharedArrayBuffer: 'readonly',
    },
    parserOptions: {
      ecmaVersion: 2018,
    },
    rules: {
      'no-labels': 'off',
      'no-restricted-syntax': 'off',
      'no-constant-condition': 'off',
      'no-plusplus': 'off',
      'no-param-reassign': 'off',
    },
  };
  