import { Client, Message } from 'discord.js';
import { Db } from 'mongodb'

module.exports.run = async (bot:Client, msg:Message, args:string[], db:Db) => {
    if(msg.author.id != process.env.IWA)return;
    let dbEmbed = await db.collection('msg').findOne({ _id: args[0] })
    if(!dbEmbed) return msg.reply("the message doesn't exist!")
    let fetchMsg = await msg.channel.messages.fetch(args[0])

    if(msg.deletable && fetchMsg.deletable) {
        try {
            await msg.delete()
            await fetchMsg.delete()
        } catch (ex) {
            console.error(ex)
        }
    }

    await db.collection('msg').deleteOne({ _id: args[0] })
};

module.exports.help = {
    name: 'delembed',
    usage: "?delembed"
};