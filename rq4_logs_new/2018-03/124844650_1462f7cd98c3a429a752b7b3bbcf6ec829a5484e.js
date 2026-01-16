const HtmlWebpackPlugin = require('html-webpack-plugin')
const CopyWebpackPlugin = require('copy-webpack-plugin')
const CleanWebpackPlugin = require('clean-webpack-plugin')
const webpack = require('webpack')
const path = require('path')
const glob = require('glob')

const PATHS = {
    src: path.join(__dirname, '../src/'),
    src_js: path.join(__dirname, '../src/js'),
    src_html: path.join(__dirname, '../src/html'),
    src_css: path.join(__dirname, '../src/css'),
    src_images: path.join(__dirname, '../src/images'),
    dist: path.join(__dirname, '../dist/')
};

var bundle_ext_list=['.js','.css','.sass','.scss','.pug']
var bundle_files=[]
for ( ext in bundle_ext_list){

    var files = glob.sync(
        path.join( PATHS.src, '*' + bundle_ext_list[ext])
    )
    bundle_files = bundle_files.concat(files)

}

bundle_files = JSON.stringify( bundle_files, null, 4);

console.log("bundle files are: " + bundle_files)



module.exports = {
    context: __dirname,
    mode: 'development',
    entry: {
        js : path.join(PATHS.src_js,'/app.js')
    },
    output: {
        path: path.resolve(PATHS.dist),
        filename: 'app.[hash].[name]'
    },
    resolve: {
        extensions: ['.js', '.jsm']
    },
    devtool: 'source-map',
    module: {
        rules: [
            /*{
                test: /\.js$/,
                use: {
                    loader: 'babel-loader',
                    options: {
                        presets: [[
                            'env',{
                                debug: false,
                                modules: false,
                                targets: {
                                    browsers: ['> 20%']
                                }
                            }
                        ]]
                    }
                }   
            },*/
            {
                test: /\.pug$/,
                use: [
                    {
                        loader: 'html-loader'
                    },
                    {
                        loader: 'pug-html-loader',
                        options: {
                            data: {} // set of data to pass to the pug render.
                        }
                    }
                ]
            },
            {
                test: /\.html$/,
                use: {
                    loader: 'html-loader'
                },
            },
            {
                test: /\.(gif|png|jpe?g|svg)$/i,
                use: [
                    'file-loader',
                    {
                        loader: 'image-webpack-loader',
                        options: {
                            bypassOnDebug: true,
                        },
                    },
                ],
            },
            {
                test: /\.css$/,
                use: [
                    {
                        loader: 'style-loader'
                    },
                    {
                        loader: 'css-loader'
                    }
                ]
            },
            {
                test: /.less$/,
                use: [
                    {
                        loader: 'style-loader'
                    },
                    {
                        loader: 'css-loader',
                        options: {
                            modules: true,
                            camelCase: 'dashes',
                            localIdentName: '[path][name]__[local]'
                        }
                    },
                    {
                        loader: 'less-loader'
                    }
                ]
            },
            {
                test: /.scss$/,
                use: [
                    {
                        loader: 'style-loader'
                    },
                    {
                        loader: 'css-loader',
                        options: {
                            modules: true,
                            camelCase: 'dashes',
                            localIdentName: '[path][name]__[local]'
                        }
                    },
                    {
                        loader: 'sass-loader'
                    }
                ]
            }
        ]
    }
    ,
    plugins: [
        new CleanWebpackPlugin(
            PATHS.dist, 
            {
                root: path.resolve(__dirname, '..'),
                verbose: true, 
            }
        ),
        new HtmlWebpackPlugin({
            favicon: path.join( PATHS.src_images, '/favicon.png'),
            hash: true,
            inject: true,
            template: path.join( PATHS.src_html, '/index.html')
        }),
            /*
        new CopyWebpackPlugin([
            {
                from: path.resolve(__dirname, '../src/static/'),
                to: '.',
                ignore: ['.*']
            },
            {
                from: path.resolve( PATHS.src, 'home/*'),
                to: path.resolve( PATHS.dist, ''),
                flatten: true,
            }
        ]),
        */
        new webpack.HotModuleReplacementPlugin()
    ]
    /*
        */
};