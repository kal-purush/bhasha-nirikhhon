/// <reference types='vitest' />
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { nxViteTsPaths } from '@nx/vite/plugins/nx-tsconfig-paths.plugin';

export default defineConfig({
  root: __dirname,
  cacheDir: '../../node_modules/.vite/apps/courses-site',

  plugins: [react(), nxViteTsPaths()],

  test: {
    globals: true,
    environment: 'happy-dom',
    include: ['specs/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}'],
    setupFiles: './tests/setup.ts',
    reporters: ['default'],
    coverage: {
      reportsDirectory: '../../coverage/apps/courses-site',
      provider: 'v8',
    },
  },
});