import type { Linter } from 'eslint'

module.exports = {
  plugins: ['@graphql-eslint'],
  extends: [
    'plugin:@graphql-eslint/schema-all',
    'plugin:@graphql-eslint/operations-all',
    'plugin:@graphql-eslint/relay',
  ],
  parser: '@graphql-eslint/eslint-plugin',
  parserOptions: {
    schema: '**/schema.graphql',
  },
} satisfies Linter.Config