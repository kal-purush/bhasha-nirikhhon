import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    alias: {
      "@std/expect": "vitest",
      "@std/testing/bdd": "vitest",
    },
  },
});