import { Message } from 'discord.js';
import { NovaClient } from '../../client/NovaClient';
import { Command } from '../../types/Command';
import { ServerConfig } from '../../client/models/ServerConfig';
import { BirthdayManager } from '../../utilities/BirthdayManager';

const run = async (client: NovaClient, message: Message, config: ServerConfig, args: any[]): Promise<any> => {
	try {
		await BirthdayManager.populateCalendars(client, config.serverId);
	} catch {
		return message.channel.send({
			content: `An error ocurred running the calendar sync for this server.`
		})
	}

	return message.channel.send({
		content: `Calendar successfully synchronised ${message.guild.name}.`
	});
};

const command: Command = {
	name: 'synccalendar',
	title: 'Sync Birthday Calendar',
	description: 'Resyncs the birthday calendar for this server.',
	usage: 'synccalendar',
	example: 'synccalendar',
	admin: true,
	deleteCmd: false,
	limited: false,
	channels: ['GUILD_TEXT'],
	run: run
};

export = command;