module.exports = {
  // ESLint で使用するパーサーを指定する
  parser: "@typescript-eslint/parser",
  extends: [
    // @typescript-eslint/eslint-plugin のおすすめルールを適用する
    "plugin:@typescript-eslint/recommended",
    // Prettier と競合している ESLint のルールを無効にする
    "prettier/@typescript-eslint",
    // `eslint-config-prettier` と `eslint-plugin-prettier` を有効化する
    // ※ extends 配列の一番最後に配置すること
    "plugin:prettier/recommended" 
  ],
  parserOptions: {
    // 最新の ECMAScript を許可する
    ecmaVersion: 2018,
    // ecmaVersion を指定してもこの記述を入れておかないと import/export 解析されない
    sourceType: "module"
  },
  rules: {
    // Prettier の設定を記述していきます
    "prettier/prettier": [
      "error",
      {
        trailingComma: "all",
        endOfLine: "lf",
        semi: false,
        singleQuote: true,
        printWidth: 80,
        tabWidth: 2
      }
    ]
  },
};