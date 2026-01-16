import type { Configuration } from '@rspack/cli'
import { launchEditorMiddleware } from '@react-dev-inspector/middleware'

const isDev = process.env.NODE_ENV === 'development'
const publicPath = isDev ? '/' : '/rspack/'

const config: Configuration = {
  context: process.cwd(),
  devServer: {
    /**
     * react-dev-inspector server config for rspack
     */
    setupMiddlewares(middlewares) {
      middlewares.unshift(launchEditorMiddleware)
      return middlewares
    },
  },

  entry: {
    main: './src/main.tsx',
  },
  builtins: {
    html: [
      {
        template: './index.html',
      },
    ],
    emotion: {
      sourceMap: true,
      autoLabel: 'always',
      labelFormat: '[local]',
    },
  },
  output: {
    publicPath,
    filename: '[name].[hash].js',
    chunkFilename: '[id].[hash].chunk.js',
    assetModuleFilename: '[name].[hash].[ext]',
    uniqueName: Math.random().toString(36).slice(2),
  },
  devtool: false,
  optimization: {
    minimize: false,
  },
  module: {
    rules: [
      {
        test: /\.svg$/,
        type: 'asset',
      },

      /**
       * NOTE: the following `@react-dev-inspector/babel-plugin` is optional,
       *       only use for online demo
       */
      {
        test: isDev
          ? () => false
          : /\.tsx$/,
        loader: 'babel-loader',
        options: {
          presets: ['@babel/preset-typescript'],
          plugins: ['@react-dev-inspector/babel-plugin'],
        },
      },
    ],
  },
}

export default config