import { Command } from 'discord-akairo';
import { Message, MessageEmbed } from 'discord.js';
import { codeBlock } from '@discordjs/builders';
import prettyMs from 'pretty-ms';

export default class PingCommand extends Command {
  constructor() {
    super('ping', {
      aliases: ['ping'],
    });
  }

  private async getPing(message: Message): Promise<string[]> {
    const ws = Math.round(this.client.ws.ping);
    const sent = await message.channel.send('Calculating ping.');
    const ping = Math.round(sent.createdAt.getTime() - message.createdAt.getTime());
    const uptime = Math.round(this.client.uptime ?? 0);

    sent.delete();

    return [`Message TTL: ${ping}ms`, `WebSocket Ping: ${ws}ms`, `Uptime: ${prettyMs(uptime)}`];
  }

  async exec(message: Message): Promise<boolean> {
    const embed = new MessageEmbed().setTitle('Bot Status').setColor('#2f3136');

    const ping = await this.getPing(message);
    embed.setDescription(codeBlock(ping.join('\n')));

    message.channel.send({ embeds: [embed] });
    return true;
  }
}