const HtmlWebpackPlugin = require('html-webpack-plugin')
const {CleanWebpackPlugin} = require('clean-webpack-plugin')
const path = require('path')

const mode = process.env.NODE_ENV.toLowerCase()

module.exports = {
  mode: mode,
  entry: "./src/index.js",
  output: {
    filename: "main.js"
  },
  resolve: {
    extensions: [".tsx",".ts",".js",'.json']
  },
  module: {
    rules: [
      {
        test: /\.jsx?$/,
        exclude: /(node_modules|bower_components)/,
        use: {
          loader: 'babel-loader',
          options: {
            presets: ['@babel/preset-env']
          }
        }
      },
      {
        test: /\.tsx?$/,
        use: "ts-loader",
        exclude: /node_modules/
      },
      {
        test: /\.scss$/,
        use: [
          "style-loader",
          {
            loader: "css-loader"
          },
          {
            loader: "sass-loader",
            options: {
              implementation: require('sass')
            }
          },
          "postcss-loader"
        ]
      },
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader", "postcss-loader"]
      },
      {
        test: /\.(jpg|png|gif)$/,
        use: [
          {
            loader: "url-loader",
            options: {
              name: "[name]_[hash].[ext]",
              outputPath: "images/",
              limit: 10240
            }
          }
        ]
      }
    ]
  },
  devtool: mode === "production" ? false : "inline-source-map",
  devServer: {
    contentBase: path.join(__dirname, "../dist"),
    compress: true,
    host: "localhost",
    port: 8080
  },
  plugins: [
    new CleanWebpackPlugin(),
    new HtmlWebpackPlugin({
      template: "./public/index.html"
    })
  ]
}