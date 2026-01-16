import HtmlWebpackPlugin from 'html-webpack-plugin';
import webpack from 'webpack';

const config: webpack.Configuration = {
    mode: 'production',
    entry: './src/index.ts',
    output: {
        path: __dirname + '/dist',
        filename: 'bundle.[hash].js',
    },
    module: {
        rules: [
            {
                test: /\.(ts|tsx)$/,
                exclude: /node_modules/,
                use: ['babel-loader'],
            },
        ],
    },
    resolve: {
        extensions: ['.ts', '.js'],
    },
    plugins: [
        new HtmlWebpackPlugin({
            template: 'public/index.html',
            favicon: 'public/favicon.ico',
        }),
    ],
    devServer: {
        host: 'localhost',
        port: 8080,
        open: true,
        historyApiFallback: true,
    },
};

export default config;