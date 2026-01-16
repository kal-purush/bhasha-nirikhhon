import * as path from 'path';
import { resolve } from 'path';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

const aliases = {
  '@@app': 'src/app',
  '@@assets': 'src/assets',
  '@@entities': 'src/entities',
  '@@features': 'src/features',
  '@@pages': 'src/pages',
  '@@shared': 'src/shared',
  '@@widgets': 'src/widgets',
};

const resolvedAliases = Object.fromEntries(
  Object.entries(aliases).map(([key, value]) => [key, resolve(__dirname, value)]),
);

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      ...resolvedAliases,
    },
  },

  build: {
    outDir: 'ssr-dist',
    ssr: true,
    lib: {
      entry: path.resolve(__dirname, 'ssr.tsx'),
      name: 'Client',
      formats: ['cjs'],
    },
    rollupOptions: {
      output: {
        dir: 'ssr-dist',
        interop: 'default',
      },
    },
  },
  ssr: {
    format: 'cjs',
  },
});