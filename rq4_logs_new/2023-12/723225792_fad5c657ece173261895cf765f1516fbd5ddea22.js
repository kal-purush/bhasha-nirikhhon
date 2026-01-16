import { defineConfig } from 'rollup';
import typescript from '@rollup/plugin-typescript';
import babel from '@rollup/plugin-babel';
import nodeResolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import json from '@rollup/plugin-json';
import dts from 'rollup-plugin-dts';

export default defineConfig([
    {
        input: 'src/index.ts',
        output: {
            sourcemap: true,
            file: 'dist/index.js',
        },
        plugins: [typescript(), babel(), nodeResolve(), commonjs(), json()],
    },
    {
        input: 'src/index.ts',
        output: {
            file: 'dist/index.d.ts',
        },
        plugins: [dts()],
    },
]);