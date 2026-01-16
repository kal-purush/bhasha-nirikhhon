const HTMLWebpackPlugin = require('html-webpack-plugin')
const webpack = require('webpack');
const Path = require('path')

module.exports = {
  mode: 'production',
  entry: './src/index.js',
  module: {
    rules: [
      {
        test: /\.(js|jsx)$/,
        exclude: /node_modules/,
        use: ['babel-loader'],
      },
      {
        test: /\.css/,
        use: ['style-loader', 'css-loader'],
      },
      {
        test: /\.s[ac]ss$/i,
        use: [
          // Creates `style` nodes from JS strings
          'style-loader',
          // Translates CSS into CommonJS
          'css-loader',
          // Compiles Sass to CSS
          'sass-loader',
        ],
      },
    ],
  },
  resolve: {
    extensions: ['*', '.js', '.jsx'],
  },
  output: {
    path: Path.join(__dirname, 'build'),
    filename: 'bundle.js',
  },
  plugins: [
    new HTMLWebpackPlugin({
      template: './public/index.html',
      filename: './index.html',
    }),
    new webpack.DefinePlugin({
        NODE_ENV: JSON.stringify(process.env.NODE_ENV),      
        EMAIL_SERVICE_ID: JSON.stringify(process.env.EMAIL_SERVICE_ID),
        EMAIL_TEMPLATE_ID: JSON.stringify(process.env.EMAIL_TEMPLATE_ID),
        EMAIL_USER_ID: JSON.stringify(process.env.EMAIL_USER_ID)
    }),
    ],
  devServer: {
    static: {
      directory: Path.join(__dirname, 'public'),
    },
    port: 3000
  },
}