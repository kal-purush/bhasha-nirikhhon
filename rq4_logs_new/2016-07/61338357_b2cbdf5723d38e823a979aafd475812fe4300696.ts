/// <reference path="../typings/index.d.ts" />

import {match} from 'react-router';
import * as Express from 'express';
import * as webpack from 'webpack';
const webpackMiddleware = require('webpack-dev-middleware');

import webpackCofigs from '../webpack.config';
import {routes} from './routes';
import renderIndex from './index';
import {handleGet} from './server';

const [clientConfig, serverConfig] = webpackCofigs;
const webpackStatsOptions = {
	chunk: false,
	chunkModules: false,
	modules: false,
	source: false,
	chunkOrigins: false
};
const PORT = process.env.PORT || 8088;

const app = Express();

app.use(webpackMiddleware(webpack(clientConfig), {
    hot: true,
    historyApiFallback: true,
    status: webpackStatsOptions
}));

app.get('*', handleGet);

app.listen(PORT, (error: Error) => {
    if (error) throw error;

    console.log('Development server with [HMR] started at port', PORT);
});