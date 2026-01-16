import type { Plugin } from "vite";
import { defineConfig } from "vite";

const NavLinkPatch = (): Plugin => ({
  name: "override-target-blank",
  enforce: "pre",
  transform: (code, id) => {
    if (id.endsWith("VPLink.vue")) {
      return code.replace("_blank", "_self");
    }
  },
});

export default defineConfig({
  plugins: [NavLinkPatch()],
});