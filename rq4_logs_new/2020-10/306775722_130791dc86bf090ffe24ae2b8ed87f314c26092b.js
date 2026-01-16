module.exports = {
  env: {
    browser: true,
    commonjs: true,
    es2021: true,
  },
  extends: "eslint:recommended",
  parserOptions: {
    ecmaVersion: 12,
  },
  globals: {
    process: "readonly",
    describe: "readonly",
    it: "readonly",
    __dirname: "readonly",
  },
  plugins: ["prettier", "mocha"],
  rules: {
    "prettier/prettier": "error",
  },
};