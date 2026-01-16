import { Client, Message } from 'discord.js';
import { Db } from 'mongodb'

module.exports.run = async (bot:Client, msg:Message, args:string[], db:Db) => {
    if(msg.author.id != process.env.IWA)return;
    let embed = args.join(' ')
    embed = JSON.parse(embed)

    let sent = await msg.channel.send(embed)

    if(msg.deletable) {
        try {
            await msg.delete()
        } catch (ex) {
            console.error(ex)
        }
    }

    await db.collection('msg').insertOne({ _id: (await sent).id, channel: (await sent).channel.id })
};

module.exports.help = {
    name: 'embed',
    usage: "?embed"
};