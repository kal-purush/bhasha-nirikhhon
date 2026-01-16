import { Client, Message, MessageEmbed } from 'discord.js'
import { Db } from 'mongodb'

module.exports.run = async (bot:Client, msg:Message, args:string[], db:Db) => {
    if(args.length == 1) {
        let content = args[0]
        if(content.length > 16 || content.length < 3)
            return msg.channel.send({"embed": { "title": ":x: > **PSN ID format invalid!**", "color": 13632027 }});

        let userDB = await db.collection('user').findOne({ '_id': { $eq: msg.author.id } });
        if(userDB.psn != null)
            return msg.channel.send({"embed": { "title": ":x: > **Sorry, you can't change your PSN ID!**", "description": "**Please contact <@125325519054045184> to change.**", "color": 13632027 }});

        await db.collection('user').updateOne({ _id: msg.author.id }, { $set: { psn: content }}, { upsert: true });
        const embed = new MessageEmbed();
        embed.setAuthor("Your PSN ID is now set to : ", msg.author.avatarURL({ format: 'png', dynamic: false, size: 128 }));
        embed.setTitle(`**${content}**`)
        embed.setColor('AQUA')
        try {
            console.log(`info: psn id of ${msg.author.tag} set`)
            return await msg.channel.send(embed).then(() => { msg.delete() })
        } catch(err) {
            console.error(err);
        }
    }
};

module.exports.help = {
    name: 'setpsn',
    usage: "?setpsn (your PSN ID)",
    desc: "Register your PSN ID to Q-Bot."
};