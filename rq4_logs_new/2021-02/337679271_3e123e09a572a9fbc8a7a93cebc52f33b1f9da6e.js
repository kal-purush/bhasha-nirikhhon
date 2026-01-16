const Discord = require('discord.js');
const config = require('./config.json');

exports.run = (client, message, args) => {
    if(!args[0]) return message.channel.send('You must ask me a full question \nUsage: `c!8ball ...`');
    let replies = ["Yes", "No", "I don't know", "Ask again later", "Definitely", "Probably"];

    let result = Math.floor((Math.random() * replies.length));
    let question = args.slice(1).join(" ");

    let ballembed = new Discord.RichEmbed()
    .setFooter(`Cookie v${config.version}b`)
    .setColor("0x00AE86")
    .addField(":question: Question", args.join(" "))
    .addField(":8ball: 8ball", replies[result]);
    message.channel.send(ballembed)
}