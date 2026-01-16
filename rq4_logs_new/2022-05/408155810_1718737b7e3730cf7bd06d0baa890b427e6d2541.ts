import bolt from "@slack/bolt";
import { Settings } from "model/Settings";

const ANNOUNCE_REGEXP = /lounasbotti\sadmin\sannounce\s"((?:\w|\d)+)"\s"(.+)"/;

export default function(app: bolt.App, settings: Settings) {
	app.message(ANNOUNCE_REGEXP, async args => {
		if (args.message.subtype || !args.message.text) {
			return Promise.resolve();
		}

		if (!settings.adminUsers.includes(args.message.user)) {
			console.debug(`User ${args.message.user} is not allowed to use admin features`);
			return Promise.resolve();
		}

		const match = ANNOUNCE_REGEXP.exec(args.message.text) ?? [];
		const channelId = match[1];
		const message = match[2];

		if (!channelId || !message) {
			console.debug("No channelId or message provided!");
			return Promise.resolve();
		}

		console.debug(`Announcing on channel ${channelId}`);
		args.client.chat.postMessage({
			channel: channelId,
			text: message.replaceAll("__n__", "\n"),
			unfurl_links: false,
			mrkdwn: true
		}).catch(console.error);
	});
}