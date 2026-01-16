const { CheckerPlugin } = require("awesome-typescript-loader")
var Visualizer = require("webpack-visualizer-plugin");
const webpack = require("webpack");

var corePlugins = [
    new CheckerPlugin(),
    new webpack.DefinePlugin({
        "process.env.NODE_ENV": JSON.stringify(process.env.NODE_ENV || 'development')
    }),
    new Visualizer({
        filename: './statistics.html'
    })
];

var devePlugins = [
    // Add dev plugins here
];

var prodPlugins = [
    new webpack.optimize.UglifyJsPlugin(),
    new webpack.optimize.DedupePlugin()
];

var plugins = process.env.NODE_ENV === "production" ? corePlugins.concat(prodPlugins) : corePlugins.concat(devePlugins)

module.exports = {
    entry: [
        "./src/index.tsx"
    ],
    devServer: {
        inline: true
    },
    output: {
        filename: "bundle.js",
        path: __dirname + "/dist",
        publicPath: "/dist/"
    },
    devtool: "cheap-module-eval-source-map",
    resolve: {
        extensions: ['.tsx', '.ts', '.js', '.css']
    },
    module: {
        rules: [

            /**
             * Source map loader support for *.js files
             * Extracts SourceMaps for source files that as added as sourceMappingURL comment.
             *
             * See: https://github.com/webpack/source-map-loader
             */
            {
                enforce: 'pre',
                test: /\.js$/,
                loader: 'source-map-loader',
                exclude: /(node_modules)/
            },
            {
                enforce: 'pre',
                test: /\.tsx?$/,
                use: 'source-map-loader'
            },
            {
                test: /\.tsx?$/,
                use: 'awesome-typescript-loader',
                exclude: /(node_modules)/
            },
            /**
             * Raw loader support for *.scss files
             *
             * See: https://github.com/webpack/raw-loader
             */
            {
                test: /\.scss$/,
                loader: ['raw-loader', 'sass-loader'],
            },
            /**
            * Raw loader support for *.css files
            *
            * See: https://github.com/webpack/raw-loader
            */
            {
                test: /\.css$/,
                loader: ['file-loader', 'url-loader', 'css-loader'],
            },
        ],
    },
    plugins: plugins
};