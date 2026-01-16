/* eslint-disable */

import { App, LogLevel } from '@slack/bolt';

const app = new App({
    token: process.env.SLACK_BOT_TOKEN,
    signingSecret: process.env.SLACK_SIGNING_SECRET,
    socketMode: true,
    appToken: process.env.SLACK_APP_TOKEN,
    logLevel: LogLevel.DEBUG
});

app.event('reaction_added', async ({ event, client, logger }) => {
    if (event.reaction !== 'meow_mudamudamuda') {
        return;
    }

    if (event.item.type !== 'message') {
        return;
    }

    const response = await client.emoji.list();

    if (response.emoji === undefined) {
        return;
    }

    const emoji_names = Object.keys(response.emoji);

    console.log(emoji_names);

    const channel = event.item.channel;
    const ts = event.item.ts;

    for (let i = 0; i < 20; i++) {
        const index = Math.floor(Math.random() * emoji_names.length);
        const name = emoji_names[index];
        emoji_names.splice(index, 1);
        await client.reactions.add({
            name: name,
            channel: channel,
            timestamp: ts
        });
    }

    console.log(event);
});

(async () => {
    await app.start();

    console.log('⚡️ Bolt app is running!');
})();