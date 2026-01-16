import type * as webpack from "webpack";
import * as path from "path";
import CopyPlugin from "copy-webpack-plugin";
import { CleanWebpackPlugin } from "clean-webpack-plugin";

export const baseConfig: webpack.Configuration = {
    mode: "development",
    devtool: "source-map",
    entry: "./src/index.ts",
    output: {
        filename: "bundle.js",
        path: path.resolve(__dirname, "dist")
    },
    module: {
        rules: [
            {
                test: /\.ts$/,
                use: "ts-loader",
                exclude: /node_modules/
            },
            {
                test: /\.(png|svg|jpg|gif)$/,
                loader: "file-loader",
                options: {
                    name: "[path][name]-[hash].[ext]"
                }
            }
        ]
    },
    resolve: {
        alias: {
            "~src": path.resolve(__dirname, "src"),
            "~resources": path.resolve(__dirname, "resources")
        }
    },
    plugins: [
        new CleanWebpackPlugin(),
        new CopyPlugin({
            patterns: [{ from: "html", to: "." }]
        })
    ],
    devServer: {
        contentBase: path.join(__dirname, "dist"),
        compress: true,
        port: 9000
    }
};

export default baseConfig;