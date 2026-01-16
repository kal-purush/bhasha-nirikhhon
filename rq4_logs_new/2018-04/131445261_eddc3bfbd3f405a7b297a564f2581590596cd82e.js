module.exports = {
  extends: "standard",
  plugins: ["react"],
  parser: "babel-eslint",
  parserOptions: {
    ecmaVersion: 6,
    sourceType: "module",
    ecmaFeatures: {
      jsx: true
    }
  },
  env: {
    es6: true,
    browser: true,
    node: true,
    mocha: true
  },
  extends: ["eslint:recommended", "plugin:react/recommended"],
  rules: {
    default-case: 0 // require default case in switch statements (off by default),
    // Prettier Plugin
  // The following rules are made available via `eslint-plugin-prettier`.
  "prettier/prettier": "error",
  }
};