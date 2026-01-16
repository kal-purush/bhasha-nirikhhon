module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  plugins: [
    '@typescript-eslint',
  ],
  extends: [
    'plugin:@typescript-eslint/recommended',
    'airbnb-base',
    'airbnb-typescript/base',
  ],
  env: {
    jest: true,
  },
  parserOptions: {
    ecmaVersion: 'esnext',
    project: './tsconfig.json',
  },
  rules: {
    'global-require': 0,
    'import/no-unresolved': 0,
    'import/no-relative-parent-imports': 2,
    'no-param-reassign': 0,
    'no-shadow': 0,
    'import/extensions': 0,
    'import/newline-after-import': 0,
    'no-multi-assign': 0,
    // allow debugger during development
    'no-debugger': process.env.NODE_ENV === 'production' ? 2 : 0,
    'class-methods-use-this': 'off',
    'key-spacing': 'off',
    'no-multi-spaces': [
      'error',
      { exceptions: { VariableDeclarator: true } },
    ],
    'no-console': 'off',
    'max-len': [
      'error',
      {
        ignoreComments: true,
        code: 150,
        ignoreTrailingComments: true,
        ignoreUrls: true,
        ignoreStrings: true,
      },
    ],
    'no-unused-expressions': [
      'error',
      {
        allowShortCircuit: true,
        allowTernary: true,
      },
    ],
    semi: ['error', 'never'],
    '@typescript-eslint/semi': ['error', 'never'],
    '@typescript-eslint/no-use-before-define': 0,
    '@typescript-eslint/no-redeclare': 0,
    '@typescript-eslint/member-delimiter-style': 'off',
    '@typescript-eslint/interface-name-prefix': 'off',
    '@typescript-eslint/no-var-requires': 0,
    '@typescript-eslint/type-annotation-spacing': 1,
    '@typescript-eslint/naming-convention': [
      'error',
      {
        selector: 'variable',
        types: ['boolean'],
        format: ['PascalCase'],
        prefix: ['is', 'should', 'has', 'can', 'did', 'will'],
      },
    ],
    curly: ['error', 'all'],
  },
  overrides: [
    {
      files: ['*.e2e.js', '**/*.test.ts', '*.test.ts'],
      env: {
        jest: true,
      },
    },
  ],
}