const path = require("path");
const htmlWebpackPlugin = require("html-webpack-plugin");

const webpack = require("webpack");
const { CleanWebpackPlugin } = require("clean-webpack-plugin");

const config = {
  entry: "./src/js/index.js",

  output: {
    path: path.resolve(__dirname, "../dist"),
    filename: "js/bundle.js",
  },

  module: {
    rules: [
      {
        test: /\.js$/i,
        exclude: /node_modules/,
        use: {
          loader: "babel-loader",
          options: {
            presets: ["@babel/preset-env"],
            plugins: [
              [
                "@babel/plugin-transform-runtime",
                {
                  regenerator: true,
                },
              ],
            ],
          },
        },
      },
    ],
  },

  plugins: [
    new htmlWebpackPlugin({
      template: "src/index.html",
      filename: "index.html",
    }),
    new CleanWebpackPlugin({ root: path.join(__dirname, "dist") }),
    new webpack.SourceMapDevToolPlugin({}),
  ],
};

module.exports = config;