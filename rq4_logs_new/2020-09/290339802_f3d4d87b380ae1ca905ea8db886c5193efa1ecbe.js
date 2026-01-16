const { merge } = require('webpack-merge');
const config = require('./webpack.config.js');
const zopfli = require('@gfx/zopfli');
const CompressionPlugin = require('compression-webpack-plugin');

module.exports = merge(config, {
  mode: 'production', 
  plugins: [
    new CompressionPlugin({
      compressionOptions: {
        numiterations: 15,
      },
      algorithm(input, compressionOptions, callback) {
        return zopfli.gzip(input, compressionOptions, callback);
      },
    }),
  ],
});