import Discord from 'discord.js';
import { Command } from '../../command';

export default class RankCommand extends Command {
  constructor(db: import('../../bot-db').BotDb) {
    super(
      {
        name: 'rank',
        description: 'Get your current rank',
        group: 'general',
        guildOnly: false,
      },
      db,
    );
  }

  public async execute(message: Discord.Message, _: string[]) {
    const { row } = await this.db.getUserRow(message.guild.id, message.author.id);

    const rankEmbed = new Discord.RichEmbed()
      .setColor('#4fa636')
      .setTitle(`@${message.author.username}'s rank!`)
      .setThumbnail(message.author.displayAvatarURL)
      .addField('Level', row.level, true)
      .addField('Exp', `${row.exp} XP`, true)
      .setTimestamp();

    await message.channel.send(rankEmbed);
  }
}