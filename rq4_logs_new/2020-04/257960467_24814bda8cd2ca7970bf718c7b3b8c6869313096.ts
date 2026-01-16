import * as path from "path";
import * as webpack from "webpack";
import * as HtmlWebpackPlugin from "html-webpack-plugin";
import { CleanWebpackPlugin } from "clean-webpack-plugin";
import * as MiniCssExtractPlugin from "mini-css-extract-plugin";

const isDev = process.env.NODE_ENV !== "production";

const config: webpack.Configuration = {
  context: path.resolve(__dirname, "src"),
  entry: "./index.tsx",
  output: {
    filename: "app.[hash].js",
    path: path.resolve(__dirname, "dist"),
  },
  devServer: {
    contentBase: "../dist",
    hot: true,
  },
  resolve: {
    extensions: [".tsx", ".ts", ".js", ".jsx", ".json"],
  },
  devtool: "inline-source-map",
  module: {
    rules: [
      {
        test: /\.s?css$/i,
        use: [
          {
            loader: MiniCssExtractPlugin.loader,
            options: {
              hmr: isDev,
              reloadAll: true,
            },
          },
          "css-loader",
          "sass-loader",
        ],
      },
      {
        test: /\.tsx?$/i,
        use: ["ts-loader"],
        exclude: /node_modules/,
      },
      { enforce: "pre", test: /\.js$/, loader: "source-map-loader" },
    ],
  },
  plugins: [
    new CleanWebpackPlugin(),
    new HtmlWebpackPlugin({
      title: "Codenames",
      template: "./index.html",
    }),
    new MiniCssExtractPlugin({
      filename: isDev ? "[name].css" : "[name].[hash].css",
      chunkFilename: isDev ? "[id].css" : "[id].[hash].css",
      ignoreOrder: false,
    }),
  ],
};

export default config;