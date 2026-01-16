'use strict';
const webpack = require('webpack');
const ExtractTextPlugin = require('extract-text-webpack-plugin');
const path = require('path');
module.exports =  {
  entry: {
		bundle: "./productsMoveUp.js"
	},
	plugins: [new ExtractTextPlugin("index.css")],
  module:{
  loaders: [
    {
      test: /\.js$/,
      exclude: /(node_modules)/,
      loader: 'babel-loader'
    },
		{ test: /\.css$/, loader:ExtractTextPlugin.extract("css-loader") }
  ]
  },
  externals: {
    'react': 'commonjs react', 
		'lodash':'commonjs lodash',
		'react-addons-css-transition-group':'commonjs react-addons-css-transition-group'

  },
	output:{
    libraryTarget: 'umd',
  	filename: 'index.js',	
		path: path.resolve(__dirname, 'dist')
	},
  resolve: {
    extensions: ['.js', '.jsx']
  },
}