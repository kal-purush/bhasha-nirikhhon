import { App, ExpressReceiver } from '@slack/bolt';
import clientConfig from '../config/slackClientConfig';

// initialize a custom express receiver to use express.js
const expressReceiver = new ExpressReceiver({
    signingSecret: clientConfig.signingSecret || '',
    processBeforeResponse: true,
});

// initializes the slack app with the bot token and the custom receiver
const slackBoltApp = new App({
    receiver: expressReceiver,
    ...clientConfig,
});

const expressApp = expressReceiver.app;

const slackWebClient = slackBoltApp.client;

export { slackBoltApp, expressApp, slackWebClient };