import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import zipPack from "vite-plugin-zip-pack";

export default ({ mode }) => {
    return defineConfig({
        plugins: [react(), zipPack()],
        define: {
            "process.env.NODE_ENV": `"${mode}"`,
        }
    })
}