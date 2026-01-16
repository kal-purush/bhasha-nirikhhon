import path from "node:path";
import url from "node:url";
import ts from "typescript-eslint";

import lichthagel, { FlatConfigItem } from "./dist/index.js";

export default [
  ...(await lichthagel({
    node: true,
    typescript: true,
  })),
  {
    languageOptions: {
      parserOptions: {
        project: true,
        tsconfigRootDir: path.dirname(url.fileURLToPath(import.meta.url)),
      },
    },
    rules: {
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
    },
  },
  {
    files: ["eslint.config.js"],
    ...ts.configs.disableTypeChecked as FlatConfigItem,
  },
  {
    files: ["src/typegen.d.ts"],
    rules: {
      "unicorn/no-abusive-eslint-disable": "off",
    },
  },
  {
    ignores: ["dist/**/*"],
  },
] satisfies FlatConfigItem[];