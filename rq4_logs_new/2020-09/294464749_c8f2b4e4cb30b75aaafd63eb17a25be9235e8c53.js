module.exports = {
    parser: '@typescript-eslint/parser',
    env: {
      'es6': true,
      'browser': true,
    },
    parserOptions: {
      ecmaFeatures: { jsx: true },
    },
    extends: [
      'eslint:recommended',
      'plugin:@typescript-eslint/recommended',
      'plugin:react/recommended',
      'plugin:prettier/recommended',
    ],
    plugins: ['react-hooks'],
    rules: {
      'react/prop-types': 'off',
      '@typescript-eslint/explicit-function-return-type': 'off',
      'react-hooks/rules-of-hooks': 'error',
      'react-hooks/exhaustive-deps': 'warn'
    },
    settings: {
      react: {
        "version": "detect",
      }
    },
  };