import { http, params } from "@serverless/cloud"; // api, data, schedule,
import { GenericMessageEvent } from "@slack/bolt";
import pkg from "@slack/bolt";
const { App, ExpressReceiver } = pkg;

const receiver = new ExpressReceiver({
  signingSecret: params.SLACK_SIGNING_SECRET,
});

const app = new App({
  token: params.SLACK_BOT_TOKEN,
  receiver,
  processBeforeResponse: true,
});

// Listens to incoming messages that contain "hello"
app.message("hello", async ({ message, say }) => {
  // say() sends a message to the channel where the event was triggered
  const msg = message as GenericMessageEvent;
  await say(`Hey there <@${msg.user}>!`);
});

http.use(receiver.app);