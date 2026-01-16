import * as minimist from 'minimist';
let webpack = require('webpack');
let path = require('path');
let clone = require('js.clone');
let webpackMerge = require('webpack-merge');
let timestamp=(new Date()).getTime();
let HtmlWebpackPlugin = require("html-webpack-plugin");
let publicPath: string ="/client/";
const CopyWebpackPlugin = require("copy-webpack-plugin");
const ExtractTextPlugin = require('extract-text-webpack-plugin');
// const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;
const shellCSS = new ExtractTextPlugin({filename:'shell.css',allChunks: true});
const mainCSS = new ExtractTextPlugin({filename:'g_css_'+timestamp+'.css',allChunks: true});
const argv: any = minimist(process.argv.slice(2));
export let isDev =false;
if(argv && argv.env && argv.env.isDev){
  isDev = JSON.parse(argv.env.isDev);
}

export var commonPlugins = [
  new webpack.ContextReplacementPlugin(
    // The (\\|\/) piece accounts for path separators in *nix and Windows
    /(.+)?angular(\\|\/)core(.+)?/,
    root('./src'),
    {
      // your Angular Async Route paths relative to this root directory
    }
  ),
  new webpack.ContextReplacementPlugin(
    // fixes WARNING Critical dependency: the request of a dependency is an expression
    /(.+)?express(\\|\/)(.+)?/,
    root('./src'),
    {
      // your Angular Async Route paths relative to this root directory
    }
  ),
  // Loader options
  new webpack.LoaderOptionsPlugin({

  })

];

export var commonConfig = {
  // https://webpack.github.io/docs/configuration.html#devtool
  devtool: isDev ? 'source-map' : '',
  resolve: {
    extensions: ['.ts', '.js', '.json'],
    modules: [ root('node_modules') ]
  },
  context: __dirname,
  output: {
    publicPath: '',
    filename: '[name].bundle.js'
  },
  module: {
    rules: [
      // TypeScript
      { test: /\.ts$/,   use: ['awesome-typescript-loader', 'angular2-template-loader'] },
      { test: /\.html$/, use: 'raw-loader' },
      { test: /shell.css/,
        loader: shellCSS.extract({
          use: ['raw-loader']
      })},
      { test: /main.css/,
        loader: mainCSS.extract({
          use: ['raw-loader']
      })},
      { test: /\.json$/, use: 'json-loader' }

      //ToDo: XML loader
    ],
  },
  plugins: [
    shellCSS,
    mainCSS
    // Use commonPlugins.
  ]

};

// Client.
export var clientPlugins = [
  new webpack.DefinePlugin({
    __BROWSER_BUILD__: true
  }),
  new HtmlWebpackPlugin({
    filename: "../index.html",
    inject: false,
    minify: {
      removeAttributeQuotes: false,
      collapseWhitespace: true
    },
    template: "src/index.ejs",
    preload: {
      images: [
      ],
      fonts: [
      ]
    },
    preconnect: []
  }),
  new CopyWebpackPlugin([
    { from: "src/static/fonts", to: "static/fonts" },
    { from: "src/static/images", to: "static/images" }
  ]),
  // new BundleAnalyzerPlugin({openAnalyzer: false})
];
export var clientConfig = {
  target: 'web',
  entry: './src/client',
  output: {
     filename: "[name]_[hash].js",
    path: root('dist/client'),
    publicPath: publicPath
  },
  node: {
    global: true,
    crypto: 'empty',
    __dirname: true,
    __filename: true,
    process: true,
    Buffer: false,
    fs: 'empty'
  }
};


// Server.
export var serverPlugins = [
   new webpack.DefinePlugin({
    __BROWSER_BUILD__: false
  }),

];
export var serverConfig = {
  target: 'node',
  entry: './src/server', // use the entry file of the node server if everything is ts rather than es5
  output: {
    filename: 'index.js',
    path: root('dist/server'),
    libraryTarget: 'commonjs2'
  },
  module: {
    rules: [
      { test: /@angular(\\|\/)material/, use: "imports-loader?window=>global" }
    ],
  },
  externals: includeClientPackages(
    /@angularclass|@angular|angular2-|ng2-|ng-|@ng-|angular-|@ngrx|ngrx-|@angular2|ionic|@ionic|-angular2|-ng2|-ng/
  ),
  node: {
    global: true,
    crypto: true,
    __dirname: true,
    __filename: true,
    process: true,
    Buffer: true,
    fs: true
  }
};

export default [
  // Client
  webpackMerge(clone(commonConfig), clientConfig, { plugins: clientPlugins.concat(commonPlugins) }),

  // Server
  webpackMerge(clone(commonConfig), serverConfig, { plugins: serverPlugins.concat(commonPlugins) })
];





// Helpers
export function includeClientPackages(packages, localModule?: string[]) {
  return function(context, request, cb) {
    if (localModule instanceof RegExp && localModule.test(request)) {
      return cb();
    }
    if (packages instanceof RegExp && packages.test(request)) {
      return cb();
    }
    if (Array.isArray(packages) && packages.indexOf(request) !== -1) {
      return cb();
    }
    if (!path.isAbsolute(request) && request.charAt(0) !== '.') {
      return cb(null, 'commonjs ' + request);
    }
    return cb();
  };
}

export function root(args) {
  args = Array.prototype.slice.call(arguments, 0);
  return path.join.apply(path, [__dirname].concat(args));
}