module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  extends: [
    'eslint:recommended',
    'plugin:react/recommended',
    'plugin:react-hooks/recommended',
    'prettier',
    '@react-native-community',
  ],
  plugins: ['react', 'react-hooks', 'prettier','simple-import-sort'],
  rules: {
    'prettier/prettier': ['error',{},{usePrettierrc: true}],
    'simple-import-sort/imports':'error',
  },
  settings: {
    react: {
      version: 'detect',
    },
  },
};