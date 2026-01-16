var path = require('path');

module.exports = {

  entry: './demo/demo.js',

  output: {
    path: path.join(__dirname, 'demo'),
    filename: 'demo.bundle.js'
  },

  module: {
    loaders: [
      { test: /\.scss$/, loader: 'style!css!sass' }
    ]
  }

};