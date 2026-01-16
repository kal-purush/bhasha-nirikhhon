import {Config as KarmaConfig, ConfigOptions} from 'karma';
import {Configuration as WebpackConfig} from 'webpack';
import webpackConfigs from './webpack.config';

const clientConfig = webpackConfigs[0];

// Enzyme externals
clientConfig.externals = {
    'cheerio': 'window',
    'react/addons': true,
    'react/lib/ExecutionEnvironment': true,
    'react/lib/ReactContext': true
};

interface WebpackKarmaConfigOptions extends ConfigOptions {
    webpack: WebpackConfig,
    webpackMiddleware?: {}
}
interface WebpackKarmaConfig extends KarmaConfig {
    set: (config: WebpackKarmaConfigOptions) => void;
}

module.exports = function(config: WebpackKarmaConfig) {
    config.set({
        files: [
            'src/**/*.test.tsx'
        ],
        preprocessors: {
            'src/**/*.test.tsx': ['webpack']
        },
        webpack: clientConfig,
        webpackMiddleware: {
            stats: {
                chunk: false,
                chunkModules: false,
                modules: false,
                source: false,
                chunkOrigins: false
            }
        },
        browsers: ['Chrome'],
        frameworks: ['mocha']
    });
};