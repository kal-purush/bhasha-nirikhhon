import { defineConfig, splitVendorChunkPlugin } from "vite";

export default defineConfig ({
    base:'/MoeZayKha/',
    build:{
        chunkSizeWarningLimit: 1600,
        plugins: [splitVendorChunkPlugin()]
    }
})