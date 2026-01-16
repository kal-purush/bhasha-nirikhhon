import { defineConfig } from "@pandacss/dev"

import { keyframes, semanticTokens } from "./theme"

export default defineConfig({
  eject: true,
  presets: [],
  hash: {
    className: true,
    cssVar: false,
  },
  minify: true,
  lightningcss: true,
  include: ["./src/**/*.{ts,tsx,js,jsx,astro}", "./pages/**/*.{ts,tsx,js,jsx,astro}"],
  importMap: "@/panda",
  theme: {
    keyframes,
    semanticTokens,
  },
  globalCss: {
    body: {
      height: "100dvh",
      backgroundColor: "{colors.background}",
    },
  },
  conditions: {
    light: ".light &",
    dark: '.dark &, [data-theme="dark"] &',
  },
  outdir: "styled-system",
  syntax: "template-literal",
  jsxFramework: "react",
})