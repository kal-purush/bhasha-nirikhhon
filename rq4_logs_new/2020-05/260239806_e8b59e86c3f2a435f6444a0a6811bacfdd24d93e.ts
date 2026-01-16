import { Client, Message, MessageEmbed } from 'discord.js'

module.exports.run = async (bot:Client, msg:Message) => {
    const embed = new MessageEmbed();
    embed.setColor('#F2DEB0')
    embed.setDescription(`**<@${msg.author.username}>**, facepalm!`)
    embed.setImage('https://i.imgur.com/zqUDpIk.gif');

    return msg.channel.send(embed)
    .then(() => {
        console.log(`info: facepalm by ${msg.author.tag}`);
    })
    .catch(console.error);
};

module.exports.help = {
    name: 'facepalm',
    usage: "?facepalm",
    desc: "Do a facepalm"
};