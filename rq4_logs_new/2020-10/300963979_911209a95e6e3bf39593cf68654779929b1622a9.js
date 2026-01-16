/* eslint-env node */
import commonjs from '@rollup/plugin-commonjs';
import gzip from 'rollup-plugin-gzip';
import html2 from 'rollup-plugin-html2';
import livereload from 'rollup-plugin-livereload';
import postcss from 'rollup-plugin-postcss';
import preprocess from 'svelte-preprocess';
import resolve from '@rollup/plugin-node-resolve';
import serve from 'rollup-plugin-serve';
import svelte from 'rollup-plugin-svelte';
import { terser } from 'rollup-plugin-terser';
import typescript from '@rollup/plugin-typescript';
import yaml from '@rollup/plugin-yaml';

const production = !process.env.ROLLUP_WATCH;

export default {
  external: [],
  input: 'src/main.ts',
  output: {
    dir: 'public',
    entryFileNames: production ? '[name].[hash].js' : '[name].js',
    format: 'iife',
    name: 'Plumeria',
    sourcemap: !production,
    globals: {}
  },
  plugins: [
    svelte({
      preprocess: preprocess({
        typescript: { tsconfigFile: './tsconfig.json' },
      }),
      dev: !production,
      emitCss: true,
    }),
    postcss({
      plugins: [],
      extract: true,
      minimize: !production,
      sourceMap: !production,
    }),

    resolve({
      browser: true,
      preferBuiltins: true,
      dedupe: ['svelte']
    }),
    commonjs(),
    typescript({ noEmitOnError: false }),
    html2({
      template: 'src/index.html',
      fileName: 'index.html',
      title: 'Griffin',
      externals: [
        { type: 'js', file: production ? '/env/production.env.js' : '/env/development.env.js', pos: 'before' },
        { type: 'js', file: '/vendor/aws-sdk-2.666.0.js', pos: 'before' },
        { type: 'js', file: 'https://cdn.bitmovin.com/player/web/8/bitmovinplayer.js', pos: 'before' },
      ]
    }),
    yaml(),

    !production && serve({
      contentBase: 'public',
      historyApiFallback: true,
    }),

    // Watch the `public` directory and refresh the
    // browser on changes when not in production
    !production && livereload({
      watch: 'public',
    }),

    // If we're building for production (npm run build
    // instead of npm run dev), minify
    production && terser(),
    production && gzip(),
  ],
  watch: {
    clearScreen: false,
    exclude: 'node_modules/**',
  }
};