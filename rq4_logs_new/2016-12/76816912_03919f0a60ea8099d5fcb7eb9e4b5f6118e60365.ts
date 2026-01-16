/**
 * Created by Daniel Fallon on 12/22/2016.
 */

import {Configuration as WebpackConfig} from 'webpack';
import {SourceRoot, DistRoot,ProjectRoot} from '../environment';
import optimize = webpack.optimize;
import ts = require('typescript');
import webpack = require('webpack');
import path = require('path');
let devCompilerOptions = {
    baseUrl: SourceRoot,
    module: "commonJs",
    target: "es2016",
    noImplicitAny: false,
};

export let DevClientConfig: WebpackConfig = {
    target: 'web',
    context: SourceRoot,
    entry: {
        app: './client/app'
    },
    output:{
        filename: '[name].js',
        chunkFilename: '[name].[chunkHash].js',
        path: path.resolve(DistRoot, 'public'),
        publicPath:  "/assets/"
    },
    resolve: {
        extensions: ['.ts', '.tsx','.js']
    },
    plugins:[
        new webpack.HotModuleReplacementPlugin(),
    ],
    module: {
        rules: [
            {
                test: /\.tsx?$/,
                use: [{
                    loader: 'babel-loader',
                    options: {
                        presets: ["es2015","react"]
                    }
                }, {
                    loader: 'ts-loader',
                    options: {
                        compilerOptions: devCompilerOptions
                    }
                }]
            }
        ]
    }

};

export let DevServerConfig: WebpackConfig  = {
    target: 'node',
    context: SourceRoot,
    entry:{
        index: './server/dev',
    },
    externals: /^[^.]/,
    output:{
        filename: "[name].js",
        path: path.resolve(DistRoot, "server"),
        libraryTarget: "commonjs"
    },
    resolve: {
        extensions: ['.ts','.js']
    },
    module: {
        rules: [
            {
                test: /\.ts$/,
                use: [{
                    loader: 'babel-loader',
                    options: {
                        presets: ["es2015"]
                    }
                }, {
                    loader: 'ts-loader',
                    options: {
                        configFileName: "tsconfig.server.json"
                    }
                }],
            },
        ]
    }
};