import { Linter } from "eslint";

const config: Linter.Config = {
  extends: ["eslint:recommended", "plugin:@typescript-eslint/recommended"],
  parser: "@typescript-eslint/parser",
  plugins: ["@typescript-eslint", "import", "unused-imports", "sort-exports"],
  rules: {
    "no-unused-vars": "error",
    "@typescript-eslint/consistent-type-imports": "error",
    "unused-imports/no-unused-imports": "error",
    "import/order": [
      "error",
      {
        pathGroups: [
          {
            pattern: "react**",
            group: "external",
            position: "before",
          },
          {
            pattern: "{next**,next/**}",
            group: "external",
            position: "before",
          },
          {
            pattern: "@/**",
            group: "parent",
            position: "before",
          },
        ],
        pathGroupsExcludedImportTypes: ["react"],
        "newlines-between": "always",
        groups: [
          "builtin",
          "external",
          "internal",
          "parent",
          "sibling",
          "index",
        ],
        alphabetize: {
          order: "asc",
          caseInsensitive: true,
          orderImportKind: "asc",
        },
      },
    ],
    "sort-exports/sort-exports": [
      "error",
      {
        sortDir: "asc",
        sortExportKindFirst: "value",
      },
    ],
  },
};

module.exports = config;
export default config;