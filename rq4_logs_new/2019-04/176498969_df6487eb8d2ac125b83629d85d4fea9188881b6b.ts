import { Command } from '../../command';
import { extractUserFrom } from '../../utils';

export default class KickCommand extends Command {
  constructor(db: import('../../bot-db').BotDb) {
    super(
      {
        name: 'kick',
        description: 'Kick a user',
        group: 'Moderation',
        usage: '<user> [reason]',
        guildOnly: true,
        requiredPermissions: ['KICK_MEMBERS'],
      },
      db,
    );
  }

  public async execute(message: import('discord.js').Message, commandArgs: string[]) {
    // Verify a valid member is passed
    const memberString = extractUserFrom(commandArgs[0]);

    if (!memberString) {
      await message.channel.send("You didn't specify anybody to kick!");
      return;
    }

    const memberToBan = message.guild.member(memberString);

    if (!memberToBan) {
      await message.channel.send('That user does not exist or is not on the server!');
      return;
    }

    let reason = 'no reason';

    if (commandArgs.length > 1) {
      reason = commandArgs.slice(1).join(' ');
    }

    await memberToBan.kick(reason);
    await message.channel.send(`Successfully kicked ${memberToBan} for ${reason}!`);
  }
}