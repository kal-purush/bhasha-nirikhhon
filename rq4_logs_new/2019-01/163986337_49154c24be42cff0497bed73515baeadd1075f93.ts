import {
  SHOW_BUNDLE_ANALYZER,
  STORE_DEV_TOOLS,
  HOST,
  DEV_DEVTOOL,
  EXCLUDE_SOURCE_MAPS,
} from './constants';
import { DefinePlugin } from 'webpack';
import { root } from './helpers.js';

const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;

console.log('PRODUCTION BUILD: ', false);
console.log('AOT BUILD: ', false);
console.log('TEST BUILD: ', true);

/**
 * Variables to be exposed in runtime
 */
const CONSTANTS = {
  DEV_SERVER: JSON.stringify(false),
  ENV: JSON.stringify('develop'),
  HOST: JSON.stringify(HOST),
  PORT: "0",
  STORE_DEV_TOOLS: JSON.stringify(STORE_DEV_TOOLS)
};

/**
 * Test config
 */
const testConfig = (function webpackConfig(): WebpackConfig {
  let config: WebpackConfig = Object.assign({});

  config.mode = 'none';
  config.cache = true;
  config.target = 'web';
  config.devtool = DEV_DEVTOOL;

  config.entry = {
    main: root('./src/main.browser.ts'),
  };

  config.output = {
    path: root('dist'),
    filename: 'bundle.js',
  };

  config.performance = {
    hints: false,
  };

  config.resolve = {
    extensions: ['.ts', '.js', '.json']
  };

  config.module = {
    rules: [
      {
        test: /\.js$/,
        loader: 'source-map-loader',
        exclude: [EXCLUDE_SOURCE_MAPS],
      },
      {
        test: /\.ts$/,
        loaders: [
          '@angularclass/hmr-loader',
          `awesome-typescript-loader?{configFileName: "tsconfig.test.json"}`,
          'angular2-template-loader',
          'angular-router-loader?loader=system&genDir=compiled&aot=false',
        ],
        exclude: [/\.(e2e|d)\.ts$/],
      },
      {
        type: 'javascript/auto',
        test: /\.json/,
        exclude: /(node_modules|bower_components)/,
        use: [
          {
            loader: 'file-loader',
            options: { name: '[name].[ext]' },
          },
        ],
      },
      { test: /\.html/, loader: 'raw-loader', exclude: [root('src/index.html')] },
      { test: /\.css$/, loader: 'raw-loader' },
      {
        test: /\.scss$/,
        loaders: ['to-string-loader', 'css-loader', 'sass-loader'],
      }
    ]
  };

  config.plugins = [
    new DefinePlugin(CONSTANTS)
  ];

  if (SHOW_BUNDLE_ANALYZER) {
    config.plugins.push(new BundleAnalyzerPlugin());
  }

  return config;
})();

module.exports = testConfig;