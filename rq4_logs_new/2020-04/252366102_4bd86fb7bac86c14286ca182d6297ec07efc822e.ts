import path from 'path'

import webpack, { ConfigurationFactory, Configuration } from 'webpack'
import { Configuration as DevServerConfiguration } from 'webpack-dev-server'
import HtmlWebpackPlugin from 'html-webpack-plugin'
import MiniCssExtractPlugin from 'mini-css-extract-plugin'
import { CleanWebpackPlugin } from 'clean-webpack-plugin'

const config: ConfigurationFactory = (_env, { mode }) => {
  const plugins: Configuration['plugins'] = [
    new webpack.HotModuleReplacementPlugin(),
    new webpack.ProgressPlugin(),
    new MiniCssExtractPlugin({
      filename: '[name]-[hash].css',
      chunkFilename: '[id].css',
      ignoreOrder: false,
    }),
    new HtmlWebpackPlugin({ template: './src/index.html' }),
    new CleanWebpackPlugin(),
  ]
  const devServer: DevServerConfiguration = {
    publicPath: '/',
    contentBase: path.resolve(__filename, '../', 'public'),
    host: '0.0.0.0',
    port: 3035,
    disableHostCheck: true,
    headers: {
      'Access-Control-Allow-Origin': '*',
    },
    historyApiFallback: {
      rewrites: [{ from: /^\/*/, to: '/index.html' }],
    },
    open: false,
    inline: true,
    hot: true,
  }
  return {
    entry: {
      app: './src/index.tsx',
    },
    output: {
      publicPath: './',
      path: path.resolve(__filename, '../', 'public'),
      filename: '[name]-[hash].js',
    },
    plugins,
    module: {
      rules: [
        {
          test: /\.tsx?$/,
          loader: 'babel-loader',
          include: [path.resolve(__dirname)],
          exclude: [/node_modules/],
          options: {
            cacheDirectory: true,
            envName: mode || 'development',
          },
        },
        {
          test: /\.less$/,
          use: [
            {
              loader: MiniCssExtractPlugin.loader,
            },
            'css-loader',
            {
              loader: 'less-loader',
              // https://github.com/ant-design/ant-design/issues/7927
              options: { javascriptEnabled: true },
            },
          ],
        },
        {
          test: /\.html$/,
          loader: 'html-loader',
        },
      ],
    },
    optimization: {
      splitChunks: {
        cacheGroups: {
          vendors: {
            priority: -10,
            test: /[\\/]node_modules[\\/]/,
          },
        },

        chunks: 'async',
        minChunks: 1,
        minSize: 30000,
        name: true,
      },
    },
    devServer,
    resolve: {
      alias: {
        'react-dom': '@hot-loader/react-dom',
      },
      extensions: ['.tsx', '.ts', '.js', '.jsx'],
      modules: ['./node_modules', path.resolve(__dirname, '.', 'src')],
    },
  }
}

export default config