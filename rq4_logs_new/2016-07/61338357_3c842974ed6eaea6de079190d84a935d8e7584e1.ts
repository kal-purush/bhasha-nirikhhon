import {cloneDeep} from 'lodash';
const nodeExternals = require('webpack-node-externals');
const ExtractTextPlugin = require('extract-text-webpack-plugin');

import {
    default as clientConfig,
    CSS_MODULES_LOCAL_ID_NAME
} from './client.webpack.config';

const serverConfig = cloneDeep(clientConfig);
serverConfig.target = 'node';
serverConfig.entry = {
    server: ['src/server']
};
serverConfig.externals = [nodeExternals()];
serverConfig.module.loaders.pop(); // remove browser CSS loader
serverConfig.module.loaders.push({
    test: /\.less$/,
    loader: ExtractTextPlugin.extract(
        `css?modules&sourceMap&localIdentName=${CSS_MODULES_LOCAL_ID_NAME}!less?sourceMap`
    )
});
serverConfig.plugins.push(new ExtractTextPlugin('styles.css'));

export default serverConfig;