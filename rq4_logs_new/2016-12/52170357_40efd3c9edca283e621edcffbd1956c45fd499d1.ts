import path = require('path');
import webpack = require('webpack');
const SpritesmithPlugin = require('webpack-spritesmith');

const ENTRY_POINTS = {
  development: ['webpack-hot-middleware/client', 'react-hot-loader/patch', './client/index.tsx'],
  production: ['./client/index.tsx'],
};

const ENV = process.env.NODE_ENV || 'development';

const PLUGINS = {
  development: [
    new webpack.HotModuleReplacementPlugin(),
    new webpack.NoErrorsPlugin(),
    new webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: '\'development\'',
      },
    }),
    new SpritesmithPlugin({
      apiOptions: {
        cssImageRef: './icons.png',
      },
      src: {
        cwd: './assets/icons',
        glob: '*.png',
      },
      target: {
        css: 'assets/icons.css',
        image: 'assets/icons.png',
      },
    }),
  ],
  production: [
    new webpack.NoErrorsPlugin(),
    new webpack.optimize.DedupePlugin(),
    new webpack.optimize.UglifyJsPlugin({
      compress: {
        warnings: false,
      },
    }),
    new webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: '\'production\'',
      },
    }),
    new SpritesmithPlugin({
      apiOptions: {
        cssImageRef: './icons.png',
      },
      src: {
        cwd: path.resolve(__dirname, 'assets/icons'),
        glob: '*.png',
      },
      target: {
        css: path.resolve(__dirname, 'assets/icons.css'),
        image: path.resolve(__dirname, 'assets/icons.png'),
      },
    }),
  ],
};

const DEV_TOOLS = {
  development: 'eval',
  production: 'source-map',
};

console.log('Path', path.join(__dirname, '..', 'client'));

export default {
  devtool: DEV_TOOLS[ENV],
  entry: ENTRY_POINTS[ENV],
  module: {
    loaders: [
      {
        include: [path.join(__dirname, '..', 'client'), path.join(__dirname, '..', 'common')],
        loaders: ['react-hot-loader/webpack', 'ts'],
        test: /\.(tsx|ts|js)/,
      },
      {
        loader: 'style-loader!css-loader',
        test: /\.css$/,
      },
      {
        loader: 'json',
        test: /\.json$/,
      },
    ],
  },
  output: {
    filename: 'bundle.js',
    path: path.join(__dirname, 'build'),
    publicPath: '/build/',
  },
  plugins: PLUGINS[ENV],
  resolve: {
    extensions: ['', '.jsx', '.js', '.tsx', '.ts'],
    root: [path.resolve('client')],
  },
};