import {config as developmentConfig} from "@app/config/api/development";
import {config as productionConfig} from "@app/config/api/production";
import {config as testConfig} from "@app/config/api/test";

import App from "@app/types/app";

let config: App.IApiConfig;

switch (process.env.NODE_ENV) {
    case "test": {
        config = testConfig;
        break;
    }
    case "development": {
        config = developmentConfig;
        break;
    }
    default: {
        config = productionConfig;
        break;
    }
}

export {config};