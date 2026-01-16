import { ApplyOptions } from '@sapphire/decorators';
import { SubCommandPluginCommand, SubCommandPluginCommandOptions } from '@sapphire/plugin-subcommands';
import type { Message } from 'discord.js';
import { Args, ok } from '@sapphire/framework';
import { addPointToUserThanksStats } from '../prismaUtils/user/prismaUserUtils';
import { handleCommandUserErrors } from '../utils/handleCommandUserErrors';

@ApplyOptions<SubCommandPluginCommandOptions>({
	aliases: ['dzięki, dzieki, dzięki'],
	description: 'Komenda do dziękowania osobie za pomoc przy rozwiązaniu problemu lub doradzeniu w jakimś temacie.',
	cooldownDelay: 60,

})
export class ThanksCommand extends SubCommandPluginCommand {
	public async messageRun(message: Message, args: Args) {
		try {
			const thanksUser = (await args.pick('member')).user.id;
			await addPointToUserThanksStats(thanksUser);
			const thanksMessage = await getThanksMessage(args);
			console.log(thanksMessage);
			const messageSource = message.url;
			const author = message.author.id;

			const content = prepareFinalMessage({
				userDiscordId: thanksUser,
				userThanksMessage: thanksMessage,
				messageLink: messageSource,
				messageAuthor: author
			});
			return message.channel.send(content);
		} catch (e: any) {
			console.log(e);
			return message.channel.send(handleCommandUserErrors(e.identifier));
		}
	}
}

interface ThanksMessage {
	userDiscordId: string;
	userThanksMessage: string;
	messageLink: string;
	messageAuthor: string;
}

const getThanksMessage = async (args: Args) => {
	console.log(args);
	if (args.finished) {
		return '';
	}
	const resolver = Args.make((arg) => ok(arg));
	return args.rest(resolver).catch(() => '');
};

const prepareFinalMessage = ({ userThanksMessage, userDiscordId, messageAuthor, messageLink }: ThanksMessage) => {
	return userThanksMessage
		? `
<@${userDiscordId}> Właśnie ci podziękowano! 🎉
	
<@${messageAuthor}> Cieszy się z waszej współpracy! Link: <${messageLink}>

> ${userThanksMessage}
`
		: `
<@${userDiscordId}> Właśnie ci podziękowano! 🎉

<@${messageAuthor}> Cieszy się z waszej współpracy! Link: <${messageLink}>
`;
};