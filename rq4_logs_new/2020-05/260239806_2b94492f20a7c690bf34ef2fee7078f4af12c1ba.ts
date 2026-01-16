import { Client, Message } from 'discord.js';
import { Db } from 'mongodb'

module.exports.run = async (bot:Client, msg:Message, args:string[], db:Db) => {
    if(msg.author.id != process.env.IWA)return;
    let dbEmbed = await db.collection('msg').findOne({ _id: args[0] })
    if(!dbEmbed) return msg.reply("the message doesn't exist!")
    let fetchMsg = await msg.channel.messages.fetch(args[0])

    let emote = args[1]
    let thing = fetchMsg.reactions.cache.find(val => val.emoji.name == emote);
    await thing.remove();

    if(msg.deletable) {
        try {
            await msg.delete()
        } catch (ex) {
            console.error(ex)
        }
    }

    await db.collection('msg').updateOne({ _id: args[0] }, { $pull: { roles: {"emote": emote}}})
};

module.exports.help = {
    name: 'delrole',
    usage: "?delrole",
};