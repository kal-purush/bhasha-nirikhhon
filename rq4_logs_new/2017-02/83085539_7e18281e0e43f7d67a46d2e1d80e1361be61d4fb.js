var debug = process.env.NODE_ENV !== "production";
var webpack = require( 'webpack' );
var path = require( 'path' );
//var combineLoaders = require( 'webpack-combine-loaders' );
var HtmlWebpackPlugin = require( 'html-webpack-plugin' );
var ExtractTextPlugin = require( 'extract-text-webpack-plugin' );

module.exports = {
  context: path.join( __dirname, "app" ),
  devtool: debug ? "inline-sourcemap" : false,
  entry: debug ? [
    'webpack-dev-server/client?http://0.0.0.0:3000',
    'webpack/hot/only-dev-server',
    './js/app.js',
  ] : [
    './js/app.js'
  ],
  resolve: {
    modules: [
      path.join( __dirname, "app" ),
      path.resolve( './app/' ),
      "node_modules"
    ]
  },
  module: {
    loaders: [
      {
        test: /\.js$/,
        exclude: /(node_modules|bower_components)/,
        include: path.join( __dirname, 'app' ),
        use: {
          loader: 'babel-loader',
          options: {
            presets: ['es2015', 'stage-0'],
            plugins: ['transform-class-properties', 'transform-decorators-legacy']
          }
        }
        // loader: 'babel-loader',
        // query: {
        //   presets: ['es2015', 'stage-0'],
        //   plugins: ['transform-class-properties', 'transform-decorators-legacy'],
        // }
      },
      {
        test: /\.s?css$/,
        use: debug ? [
          'style-loader',
          'css-loader',
          'autoprefixer-loader',
          {
            loader: 'sass-loader',
            options: {
              sourceMap: true
            }
          }
        ] : ExtractTextPlugin.extract({
          fallback: 'style-loader',
          use: [
            'css-loader',
            'autoprefixer-loader',
            {
              loader: 'sass-loader',
              options: {
                sourceMap: true
              }
            }
          ]
        })
        // loader: debug ? "style!css!autoprefixer!sass?sourceMap" : ExtractTextPlugin.extract( 'style', 'css!autoprefixer!sass?sourceMap' )
      },
      {
        test: /\.html$/,
        use: [
          {
            loader: debug ? 'raw-loader' : 'html-loader',
            options: debug ? {
            } : {
              attrs: [
                'img:src',
                'source:src'
              ]
            }
          }
        ]

        // loader: debug ? "raw" : "html-loader?attrs[]=img:src&attrs[]=source:src"
      },
      {
        test: /\.jpe?g$|\.gif$|\.png$|\.svg$|\.woff$|\.ttf$|\.wav$|\.mp3$/,
        use: [
          {
            loader: 'file-loader',
            options: {
              name: 'assets/[name].[ext]',
              context: 'app/assets'
            }
          }
        ]
        // loader: "file?name=assets/[name].[ext]&context=app/assets"
      },
      {
        test: /\.mp4$/,
        use: [
          {
            loader: 'file-loader',
            options: {
              name: 'assets/[name].[ext]',
              context: 'app/assets'
            }
          }
        ]
        // loader: "file?name=assets/[name].[ext]&context=app/assets"
      }
    ]
    // loaders: [
    //   {
    //     test: /\.js$/,
    //     exclude: /(node_modules|bower_components)/,
    //     include: path.join( __dirname, 'app' ),
    //     loader: 'babel-loader',
    //     query: {
    //       presets: ['es2015', 'stage-0'],
    //       plugins: ['transform-class-properties', 'transform-decorators-legacy'],
    //     }
    //   },
    //   {
    //     test: /\.s?css$/,
    //     loader: debug ? "style!css!autoprefixer!sass?sourceMap" : ExtractTextPlugin.extract( 'style', 'css!autoprefixer!sass?sourceMap' )
    //   },
    //   {
    //     test: /\.html$/,
    //     loader: debug ? "raw" : "html-loader?attrs[]=img:src&attrs[]=source:src"
    //   },
    //   {
    //     test: /\.jpe?g$|\.gif$|\.png$|\.svg$|\.woff$|\.ttf$|\.wav$|\.mp3$/,
    //     loader: "file?name=assets/[name].[ext]&context=app/assets"
    //   },
    //   {
    //     test: /\.mp4$/,
    //     loader: "file?name=assets/[name].[ext]&context=app/assets"
    //   }
    // ]
  },
  //sassLoader: {
    //includePaths: [path.resolve( __dirname, "./app/" )]
  //},
  output: debug ? {
    path: path.join( __dirname, "app/" ),
    filename: "app.js"
  } : {
    path: path.join( __dirname, "dist/" ),
    filename: 'app.js',
  },
  plugins: debug ? [

  ] : [
    //new webpack.optimize.DedupePlugin(),
    //new webpack.optimize.OccurenceOrderPlugin(),
    // new webpack.optimize.UglifyJsPlugin({ mangle: false, sourcemap: true }),
    new webpack.BannerPlugin( '\n\nCompiled at '+ new Date() +'\n\n' ),
    new HtmlWebpackPlugin({
      template: 'index.html',
      filename: 'index.html',
      inject: false
    }),
    new ExtractTextPlugin(  {
      filename: 'main.css',
      allChunks: true
    })
  ],
};