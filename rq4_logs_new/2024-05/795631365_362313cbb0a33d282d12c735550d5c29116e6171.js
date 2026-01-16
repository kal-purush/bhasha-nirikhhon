import globals from "globals";
import js from "@eslint/js";
import ts from "typescript-eslint";
import pluginReactConfig from "eslint-plugin-react/configs/recommended.js";
import { fixupConfigRules } from "@eslint/compat";

export default [
  {
    languageOptions: { 
      globals: globals.browser, 
      parserOptions: { project: "./tsconfig.json" }, 
    }
  },
  js.configs.recommended,
  ...ts.configs.recommendedTypeChecked,
  ...ts.configs.stylisticTypeChecked,
  ...fixupConfigRules(pluginReactConfig),
  {
    rules: {
      "indent": ["warn", 2, { "SwitchCase": 1 }],
      "linebreak-style": ["warn", "unix"],
      "quotes": ["warn", "double"],
      "semi": ["warn", "always"],
      "max-len": ["warn", { "code": 120 }],
      "@typescript-eslint/consistent-type-definitions": ["off"],
      "react/react-in-jsx-scope": ["off"],
    },
    settings: {
      react: { version: "detect" }
    }
  }
];