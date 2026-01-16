// vite.config.js
import autoprefixer from 'autoprefixer';
import { resolve } from 'path';
import tailwindcss from 'tailwindcss';
import { defineConfig } from 'vite';
// import typescript from '@rollup/plugin-typescript';
// import tsConfigPaths from 'vite-tsconfig-paths';
// import { nodeResolve } from '@rollup/plugin-node-resolve';
// import { babel } from '@rollup/plugin-babel';
// import commonjs from '@rollup/plugin-commonjs';
import { UserConfigExport } from 'vite';
import dts from 'vite-plugin-dts';
const extensions = ['.js', '.jsx', '.ts', '.tsx'];
const app = async (): Promise<UserConfigExport> =>
  defineConfig({
    plugins: [
      dts({
        include: 'src',
        insertTypesEntry: true,
      }),
      // tsConfigPaths(),
      // nodeResolve({ extensions }),
      // commonjs(),
      // typescript(),
      // babel({
      //   extensions,
      //   babelHelpers: 'bundled',
      // }),
    ],
    css: {
      postcss: {
        plugins: [tailwindcss('tailwind.config.cjs'), autoprefixer],
        from: './src/tailwind.css',
        to: 'dist/index.css',
      },
    },
    resolve: {
      alias: {
        '@': resolve(__dirname, 'src'),
      },
    },
    build: {
      cssTarget: 'esnext',
      target: 'esnext',
      lib: {
        entry: resolve('src', 'index.ts'),
        name: '@namviet-fe/core-ui',
        formats: ['es', 'umd'],
        fileName: (format) => `core-ui.${format}.js`,
      },
      rollupOptions: {
        external: ['react', 'react-dom'],
        output: {
          globals: {
            react: 'react',
            'react-dom': 'react-dom',
          },
          interop: 'auto',
        },
      },
    },
  });

export default app;