const path = require('path');
//const ExtractTextPlugin = require("extract-text-webpack-plugin");

module.exports = {
  entry: './src/app.js',
  // webpack.development.config.js
  mode: 'development',
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'bundle.js'
    },
  module: {
    rules: [
      {
        test: /\.scss$/
      },
      {
        test:  /\.(gif|png|jpe?g|svg)$/i,
        use: [
          'file-loader',
          {
            loader: 'image-webpack-loader',
            options: {
              bypassOnDebug: true,
            },
          },
        ],
      }
    ]
  },
  plugins: [
    new ExtractTextPlugin('bundle.css'),
    new webpack.NamedModulesPlugin(),
  - new webpack.DefinePlugin({ "process.env.NODE_ENV": JSON.stringify("development") }),
  - new webpack.DefinePlugin({ "process.env.NODE_ENV": JSON.stringify("production") }),
  - new webpack.optimize.ModuleConcatenationPlugin(),
  - new webpack.NoEmitOnErrorsPlugin() 
  ],
performance: { 
    maxAssetSize: 100,
    maxEntrypointSize: 100,
    hints: 'warning'
   }
};