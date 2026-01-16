import path from 'node:path';
import { defineConfig } from 'vite';
import reactPlugin from '@vitejs/plugin-react-swc';

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [reactPlugin()],
  base: '/root-finder/',
  server: {
    open: true,
    port: process.env.PORT,
  },
  esbuild: {
    // Prevents minification of React Component names,
    // for a better experience with the React Dev Tools browser extension
    // Note: esbuild is for the dev build only, the production build still minifies
    keepNames: true,
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
});