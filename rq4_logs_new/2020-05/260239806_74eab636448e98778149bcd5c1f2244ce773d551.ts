import { Client, Message, MessageReaction, User, MessageEmbed } from 'discord.js'
import { Db } from 'mongodb'
const util = require('../../js/utilities')
let lastGif:number = 0, count:number = 1;
let lastGifFail:number = 0, countFail:number = 1;

module.exports.run = async (bot:Client, msg:Message, args:string[], db:Db) => {
    if(msg.channel.type != "dm")
        await msg.delete().catch(console.error)
    if(msg.mentions.everyone)return;
    let mention = msg.mentions.users.first()
    if(!mention)return;

    if(mention.id == msg.author.id)
        return msg.channel.send({"embed": { "title": `:x: > **You can't highfive youself!**`, "color": 13632027 }});

    if(mention.id == bot.user.id)return;

    let reply = await msg.channel.send({
        "embed": {
            "title": "🙌",
            "description": `<@${msg.author.id}> wants to highfive with you <@${mention.id}> !\nreact with ✋ to accept the highfive`,
            "color": 15916720,
            "footer": {
                "text": "the request will fail in 30 seconds"
            }
        }
    })

    await reply.react('✋');

    let waiter = bot.on('messageReactionAdd', async function testHighfive (reaction:MessageReaction, author:User) {
        if(reaction.emoji.name === '✋' && author.id === mention.id) {
            interceptListener(waiter, reply)

            const embed = new MessageEmbed();
            embed.setColor('#F2DEB0')

            let n = util.randomInt(count)
            while(lastGif == n)
                n = util.randomInt(count);
            lastGif = n;

            embed.setTitle(`**${msg.author.username} 🙌 ${mention.username}**`)
            embed.setImage(`https://cdn.iwa.sh/img/highfive/${n}.gif`)

            await db.collection('user').updateOne({ '_id': { $eq: msg.author.id } }, { $inc: { highfive: 1 }});
            await db.collection('user').updateOne({ '_id': { $eq: mention.id } }, { $inc: { highfive: 1 }});
            await db.collection('user').updateOne({ '_id': { $eq: bot.user.id } }, { $inc: { highfive: 1 }});

            return msg.channel.send(embed)
            .then(() => {
                console.log(`info: highfive sent by ${msg.author.tag} with ${mention.tag}`);
            })
            .catch(console.error);
        }
    })

    setTimeout(async () => {
        interceptListener(waiter, reply)

        let n = util.randomInt(countFail)
        while(lastGifFail == n)
            n = util.randomInt(countFail);
        lastGifFail = n;

        const embed = new MessageEmbed();
        embed.setColor('#F2DEB0')
        embed.setDescription(`**<@${msg.author.id}>...**`)
        embed.setImage(`https://cdn.iwa.sh/img/highfive/fail/${n}.gif`)
        return msg.channel.send(embed)
    }, 5000)
};

module.exports.help = {
    name: 'highfive',
    usage: "?highfive (mention someone)",
    desc: "highfive people by mentioning them"
};

async function interceptListener(waiter:Client, reply:Message) {
    let listeners = waiter.listeners('messageReactionAdd')
    let func:any = listeners.find(val => val.name == "testHighfive")
    if(func)
        waiter.removeListener('messageReactionAdd', func)
    if(!reply.deleted)
        await reply.delete()
}