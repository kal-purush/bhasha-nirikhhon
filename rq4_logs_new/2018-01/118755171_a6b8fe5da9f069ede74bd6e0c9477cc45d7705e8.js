import babel from 'rollup-plugin-babel';

export default {
  input: 'src/main.js',
  output: {
    format: 'iife',
    file:'dist/bundle.js',
    sourcemap:'inline'
  },
  plugins: [
   babel({
     exclude: 'node_modules/**',
   })
 ]
};