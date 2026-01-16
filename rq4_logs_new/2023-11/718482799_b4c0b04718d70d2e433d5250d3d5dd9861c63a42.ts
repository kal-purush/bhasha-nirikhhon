import * as dotenv from 'dotenv';
import * as process from "process";

let cfgResult = dotenv.config();

export interface EnvConfig {
    nebulaGraph: {
        servers: string[]
        userName: string
        password: string
        space: string
    }
    network: {
        tron: {
            fullNode: string
            apiKey: string
        }
    }
}

export function getConfig(): EnvConfig {
    if (cfgResult == null) {
        cfgResult = dotenv.config();
    }
    return {
        nebulaGraph: {
            servers: [process.env.NEBULAGRAPH_SERVERS],
            userName: process.env.NEBULAGRAPH_USERNAME,
            password: process.env.NEBULAGRAPH_PASSWORD,
            space: process.env.NEBULAGRAPH_SPACE,
        },
        network: {
            tron: {
                fullNode: process.env.NETWORK_TRON_NODE,
                apiKey: process.env.NETWORK_TRON_APIKEY
            }
        }
    }
}