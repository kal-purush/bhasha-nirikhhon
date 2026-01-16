import { Client, Message } from 'discord.js';
import { Db } from 'mongodb'

module.exports.run = async (bot:Client, msg:Message, args:string[], db:Db) => {
    if(msg.author.id != process.env.IWA)return;

    let dbEmbed = await db.collection('msg').findOne({ _id: args[0] })
    if(!dbEmbed) return msg.reply("the message doesn't exist!")
    let fetchMsg = await msg.channel.messages.fetch(args[0])

    args.shift()
    let embed = args.join(' ')
    embed = JSON.parse(embed)

    let sent = await fetchMsg.edit(embed)

    if(msg.deletable) {
        try {
            await msg.delete()
        } catch (ex) {
            console.error(ex)
        }
    }
};

module.exports.help = {
    name: 'editembed',
    usage: "?editembed"
};