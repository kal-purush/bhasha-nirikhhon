import path from "path";
import HtmlWebpackPlugin from "html-webpack-plugin";
import tailwindcss from "tailwindcss";
import { merge } from "webpack-merge";
import postcssPresetEnv from "postcss-preset-env";
import MiniCssExtractPlugin from "mini-css-extract-plugin";
import sharedConfig from "./webpack.shared.config";

import type { Configuration as DevServerConfiguration } from "webpack-dev-server";

import type { Configuration } from "webpack";
import { BannerPlugin } from "webpack";

const clientPort = 8080;
const htmlPlugin = new HtmlWebpackPlugin({
  template: "./public/index.html",
  filename: "./index.html",
});

const devServer: DevServerConfiguration = {
  static: path.join(__dirname, "build"),
  compress: true,
  port: clientPort,
  liveReload: true,
};

const config: Configuration = {
  target: "web",
  entry: "./src/index.tsx",
  module: {
    rules: [
      {
        test: /\.(ts|js)x?$/,
        exclude: /node_modules/,
        use: {
          loader: "babel-loader",
          options: {
            presets: [
              "@babel/preset-env",
              "@babel/preset-react",
              "@babel/preset-typescript",
            ],
          },
        },
      },
      {
        test: /\.css$/i,
        include: path.resolve(__dirname, "src"),
        use: [MiniCssExtractPlugin.loader, "css-loader", "postcss-loader"],
      },
    ],
  },
  resolve: {
    extensions: [".tsx", ".ts", ".js"],
  },
  output: {
    path: path.resolve(__dirname, "build/client"),
    filename: "public/client.bundle.js",
    publicPath: `http://localhost:${clientPort}/`,
  },
  devtool: "inline-source-map",
  plugins: [
    new MiniCssExtractPlugin({
      filename: "styles/bundle.css",
    }),
    htmlPlugin,
    postcssPresetEnv,
    tailwindcss,
  ],
  devServer,
};

export default merge(sharedConfig, config);