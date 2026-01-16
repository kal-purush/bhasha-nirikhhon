/**
 * Created by Daniel Fallon on 12/22/2016.
 */

import {Configuration as WebpackConfig} from 'webpack';
import {DevServerConfig} from './webpack/dev';

let PartialConfigs = {
    DevConfig: DevServerConfig,
    ProdConfig: require("./webpack/prod"),
    ServerConfig: require("./webpack/server"),
    ClientConfig: require("./webpack/client"),
};
// export function resolveConfig({development}):WebpackConfig{
//     const ifDev = then => development? then : null;
//     const ifProd = then => !development? then : null;
//
//     return PartialConfigs.DevConfig;
// }

export = DevServerConfig;