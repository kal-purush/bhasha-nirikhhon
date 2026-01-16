module.exports = {
  env: {
    browser: true,
    es2021: true
  },
  extends: [
    'standard'
  ],
  parserOptions: {
    ecmaVersion: 12,
    sourceType: 'module'
  },
  rules: {
    semi: ['error', 'always'],
    'max-lines': ['error', {
      max: 500,
      skipComments: true,
      skipBlankLines: true
    }],
    'max-len': ['error', { code: 100 }]
  }
};