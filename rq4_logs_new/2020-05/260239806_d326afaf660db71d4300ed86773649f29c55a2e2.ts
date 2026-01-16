import { Client, Message, MessageReaction } from "discord.js";

module.exports = class starboard {

    static async send (bot:Client, msg:Message, reaction:MessageReaction, content:string):Promise<void> {
        let channel:any = bot.channels.cache.find(val => val.id == process.env.STARBOARDTC);
        await msg.react(reaction.emoji.name);
        await channel.send({
            "embed": {
              "description": `${content}[message link✉️](${msg.url})`,
              "color": 14212956,
              "timestamp": msg.createdTimestamp,
              "footer": {
                "text": "New starboard entry ⭐️"
              },
              "author": {
                "name": msg.author.username,
                "icon_url": msg.author.avatarURL({ format: 'png', dynamic: false, size: 128 })
              }
            }
          });
        console.log(`info: new message into starboard (author: ${msg.author.tag})`);
    }

}