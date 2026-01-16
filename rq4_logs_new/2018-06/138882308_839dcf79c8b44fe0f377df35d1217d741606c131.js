const path = require('path');
const webpack = require('webpack');
const HtmlWebpackPlugin = require('html-webpack-plugin');

const isDev = process.env.NODE_ENV === 'development';

const config = {
  target: "web",
  entry: {
    app: path.join(__dirname, '../client/app.js')
  },
  output: {
    filename:'[name].[hash].js',
    path: path.join(__dirname, '../dist'),
    publicPath:'/public/'
  },
  module: {
    rules:[{
      test:/\.(jsx|js)$/,
      loader:'babel-loader',
      exclude:/node_modules/
    }, {
      test:/\.ejs$/,
      loader:'ejs-compiled-loader'
    }]
  },
  resolve: {
    extensions: ['.js','.jsx']
  },
  plugins: [
    new HtmlWebpackPlugin({
      template: path.join(__dirname,'../client/template.html'),
      filename:'index.html'
    }),
    new HtmlWebpackPlugin({
      // template: '!!ejs-compiled-loader!'+path.join(__dirname,'../client/server.template.ejs'),
      template: path.join(__dirname,'../client/server.template.ejs'),
      filename:'server.ejs'
    })
  ]
}

if(isDev) {
  config.entry = {
    app:[
      'react-hot-loader/patch',
      path.join(__dirname, '../client/app.js')
    ]
  };
  config.devServer = {
    contentBase:path.join(__dirname,'../dist'),
    host:'127.0.0.1',
    port:'9999',
    hot:true,
    overlay:{
      errors:true
    },
    disableHostCheck: true,
    publicPath:'/public/',
    historyApiFallback: {
      index:'/public/index.html'
    },
    proxy: {
      '/api':'http://localhost:3333'
    }
  }
  config.plugins.push(new webpack.HotModuleReplacementPlugin())
}

module.exports = config;