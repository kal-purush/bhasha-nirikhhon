import eslintPluginAstro from "eslint-plugin-astro";
export default [
  // add more generic rule sets here, such as:
  // js.configs.recommended,
  ...eslintPluginAstro.configs.recommended,
  {
    plugins: ["jsx-a11y"],
  },
  {
    rules: {
      "jsx-a11y/rule-name": 2,
      // override/add rules settings here, such as:
      // "astro/no-set-html-directive": "error"
    },
  },
];