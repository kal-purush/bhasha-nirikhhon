module.exports = {
  extends: [
    'eslint:recommended',
    'plugin:prettier/recommended',
    'plugin:@typescript-eslint/recommended',
  ],
  plugins: ['prettier', '@typescript-eslint'],
  rules: {
    eqeqeq: 'error',
    'no-console': 'off',
    'no-undef': 'off',
    'no-unused-vars': 'off',
    // i dont want to see these errors, i just want them auto-fixed
    'prettier/prettier': 'off',
    '@typescript-eslint/explicit-module-boundary-types': 'off',
    '@typescript-eslint/explicit-function-return-type': 'off',
    '@typescript-eslint/no-explicit-any': 'off',
    // VS-code already give these a different treatment
    '@typescript-eslint/no-unused-vars': 'off',
    'no-empty': 'off',
    '@typescript-eslint/no-inferrable-types': 'off',
    'no-extra-parens': 'off',
    '@typescript-eslint/no-extra-parens': 'off',
    'no-mixed-operators': 'off',
    'prefer-const': 'warn', // annoying when these get auto "fixed" bc you comment something out
    'no-debugger': 'off',
  },
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 6,
    sourceType: 'module',
  },
  env: {
    browser: true,
    node: true,
    es6: true,
    jest: true,
  },
  ignorePatterns: ['node_modules', 'build', 'dist', 'public'],
};