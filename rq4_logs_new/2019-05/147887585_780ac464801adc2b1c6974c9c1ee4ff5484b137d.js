const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin')
const webpack = require('webpack');
const CleanWebpackPlugin = require('clean-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');

const devMode = process.env.NODE_ENV === 'development'

function getDevConfig() {
   let cssLoader = [
        { loader: devMode ? 'style-loader' : MiniCssExtractPlugin.loader },
        { loader: 'css-loader' },
        // {
        //     loader: 'px2rem-loader',
        //     options: {
        //         remUnit: 37.5,
        //         remPrecision: 8
        //     }
        // },
        { 
            loader: 'postcss-loader',
            options: {
                config: {path: path.join(__dirname, '../postcss.config.js')}
            }
        }
    ]

    const lessLoader = cssLoader.concat({
        loader: 'less-loader',
        options: {
            javascriptEnabled: true
        }
    })

    return {
        entry: [
            path.resolve(__dirname, '../src/index.js')
        ],
        output: {
            path: path.resolve(__dirname, '../dist'),
            publicPath: devMode ? 'http://localhost:3000/' : './',
            // publicPath: './',
            filename: 'bundle.js',
            chunkFilename: '[id].chunk.bundle.js',
        },
        mode: 'development',
        module: {
            rules: [
                {
                    test: /\.js$/,
                    include: path.resolve(__dirname, '../src'),
                    loader: 'babel-loader',
                    exclude: /\/node_modules/
                },
                {
                    test: /\.css$/,
                    use: cssLoader
                },
                {
                    test: /\.less$/,
                    use: lessLoader                   
                },
                {
                    test: /\.(png|jpg|gif)$/i,
                    use: [
                        {
                            loader: 'url-loader',
                            options: {
                                limit: 8192
                            }
                        }
                    ]
                },
                {
                    test: /\.(woff2?|eot|ttf|otf|svg|mp3)(\?.*)?$/,
                    loader: 'file-loader',
                    options: {
                        limit: 8192,
                        name: `/assets/[name].[hash:4].[ext]`
                    }
                },
            ]
        },
        plugins: [

            new MiniCssExtractPlugin({
                    filename: '[name].[hash:4].css',
                    chunkFilename: '[name]/[id].[hash:4].css'
                }),
            new webpack.DefinePlugin({
                    'process.env': {
                        NODE_ENV: JSON.stringify(process.env.NODE_ENV)
                    }
                }),
            new HtmlWebpackPlugin({
                title: '库存管理系统',
                filename: 'index.html',
                template: path.resolve(__dirname, '../public/index.html'),
                minify: {
                    collapseWhitespace: true, //把生成的html空格去掉，减少空间
                },
                // hash: true, //为了更好的 cache，可以在文件名后加个 hash。
            }),
            // new CleanWebpackPlugin(),//实例化，参数为目录
        ]
    }
}

module.exports = getDevConfig