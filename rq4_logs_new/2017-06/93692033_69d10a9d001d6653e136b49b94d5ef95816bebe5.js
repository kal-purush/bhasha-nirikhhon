const path = require('path');

module.exports = {
  context: __dirname, // running this from our root directory always (__dirname is a Node global variable that refers to the root directory)
  entry: './js/ClientApp.jsx', // this is the front door to the project
  devtool: 'cheap-eval-source-maps', // inline source maps (which allow us to see pre-bundled code in errors) into bundled code
  output: {
    // always output to 'public/bundle.js'
    path: path.join(__dirname, 'public'),
    filename: 'bundle.js'
  },
  resolve: {
    extensions: ['.js', '.jsx', '.json'] // the order of resolution that Webpack will try different file extensions before it finds the correct one
  },
  stats: {
    // things we want reported back to us when building
    colors: true, // colors!
    reasons: true, // more useful error output
    chunks: true // tells you about code being broken up into chunks
  },
  module: {
    rules: [
      // array of rules that Webpack will use to apply different loaders (e.g. Babel loader) to the code
      {
        test: /\.jsx?$/, // Regex that says extension for a file must be .js and possibly x (i.e. .js and .jsx) in order to be run through the loader below
        loader: 'babel-loader' // the loader we want to run code through
      }
    ]
  }
};