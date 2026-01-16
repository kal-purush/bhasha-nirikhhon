import path from 'path'
import { Configuration, ProgressPlugin } from 'webpack'
import type { Configuration as DevServerConfiguration } from 'webpack-dev-server'
import ESLintWebpackPlugin from 'eslint-webpack-plugin'
import HtmlWebPackPlugin from 'html-webpack-plugin'
import HtmlWebpackHarddiskPlugin from 'html-webpack-harddisk-plugin'

const IS_DEV = process.env.NODE_ENV != 'production'
const IS_DEV_SERVER = process.env.WEBPACK_SERVE

const resolve = (...paths: string[]): string => path.resolve(__dirname, ...paths)

export function baseURL(path: string, isDev: boolean = IS_DEV): string {
  const p = path.replace(/^\//, '')
  if (isDev && IS_DEV_SERVER) {
    return `http://localhost:3000/${p}`
  } else {
    return p
  }
}

type WebpackConfig = Configuration & { devServer?: DevServerConfiguration }

const webpack_config: WebpackConfig = {
  devtool: 'source-map',
  entry: [
    'webpack-dev-server/client?http://localhost:3000',
    'webpack/hot/only-dev-server?http://localhost:3000',
    resolve('./src/app/index.ts')
  ],
  output: {
    path: resolve('../public'),
    filename: 'js/[name].js',
    publicPath: baseURL('/')
  },
  devServer: {
    liveReload: true,
    host: '0.0.0.0',
    port: 3000,
    compress: true,
    hot: true,
    historyApiFallback: true,
    client: {
      logging: 'verbose',
      overlay: true,
      progress: true
    },
    allowedHosts: '*',
    static: {
      publicPath: '/'
    },
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    webSocketServer: 'ws'
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/i,
        loader: 'ts-loader',
        include: resolve('./src'),
        exclude: /node_modules/
      },
      {
        test: /\.css$/i,
        use: ['style-loader', 'css-loader', 'postcss-loader'],
        exclude: /node_modules/,
        include: resolve('./src')
      },
      {
        test: /\.html$/i,
        exclude: /node_modules/,
        include: resolve('./src'),
        use: {
          loader: 'html-loader',
          options: { minimize: false }
        }
      }
    ]
  },
  resolve: {
    extensions: ['.ts', '.tsx', '.js'],
    alias: {
      '@': resolve('./src/app')
    }
  },
  plugins: [
    new ESLintWebpackPlugin({
      context: resolve('./src'),
      extensions: ['ts', 'tsx'],
      fix: true,
      threads: true
    }),
    new ProgressPlugin(),
    new HtmlWebPackPlugin({
      template: resolve('./src/html/index.html'),
      alwaysWriteToDisk: true
    }),
    new HtmlWebpackHarddiskPlugin({
      outputPath: resolve('../views')
    })
  ]
}

module.exports = () => {
  if (!IS_DEV) {
    webpack_config.mode = 'production'
  } else {
    webpack_config.mode = 'development'
  }
  return webpack_config
}