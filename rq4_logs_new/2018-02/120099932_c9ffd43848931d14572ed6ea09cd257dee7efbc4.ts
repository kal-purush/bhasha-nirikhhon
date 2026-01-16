import * as appRoot from "app-root-path";
import * as config from "config";
import { UpdateOnPush } from "./handlers/event/UpdateOnPush";
import { UpdateOnRelease } from "./handlers/event/UpdateOnRelease";
import { UpdateOnTag } from "./handlers/event/UpdateOnTag";
import { UpdatePolicy } from "./start.client";

// tslint:disable-next-line:no-var-requires
const pj = require(`${appRoot.path}/package.json`);

const token = process.env.GITHUB_TOKEN;

const notLocal = process.env.NODE_ENV === "production" || process.env.NODE_ENV === "staging";

const events = [];

switch (UpdatePolicy.toLowerCase()) {
    case "push":
        events.push(() => new UpdateOnPush());
        break;
    case "tag":
        events.push(() => new UpdateOnTag());
        break;
    case "release":
        events.push(() => new UpdateOnRelease());
        break;
}

export const configuration: any = {
    name: pj.name,
    version: pj.version,
    teamIds: process.env.TEAM_ID,
    commands: [
    ],
    events,
    token,
    http: {
        enabled: false,
    },
    endpoints: {
        graphql: config.get("endpoints.graphql"),
        api: config.get("endpoints.api"),
    },
    cluster: {
        enabled: false,
        // worker: 2,
    },
    ws: {
        enabled: true,
        termination: {
            graceful: false,
        },
    },
};