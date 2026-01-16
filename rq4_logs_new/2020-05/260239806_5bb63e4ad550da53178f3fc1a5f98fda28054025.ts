import { Client, Message } from 'discord.js'
import { Db } from 'mongodb'

module.exports.run = async (bot:Client, msg:Message, args:string[], db:Db) => {
    let user = await db.collection('user').findOne({ '_id': { $eq: msg.author.id } });
    let avatar = msg.author.avatarURL({ format: 'png', dynamic: false, size: 128 })
    if(!user.psn)
        return msg.channel.send({
            "embed": {
              "title": "Do `?setpsn (your id)` to register it.",
              "color": 15802940,
              "author": {
                "name": "It looks like you haven't yet registered your PSN ID!",
                "icon_url": avatar
              }
            }
          })
    else
        return msg.channel.send({
        "embed": {
          "title": `**${user.psn}**`,
          "color": 3374298,
          "author": {
            "name": `${msg.author.username}'s PSN ID`,
            "icon_url": avatar
          }
        }
      })
};

module.exports.help = {
    name: 'psn',
    usage: "?psn",
    desc: "Print your PSN ID in the channel you sent the command."
};