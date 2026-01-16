
const Discord = require("discord.js")
const randomColor = require('randomcolor'); // import the script

module.exports.run = async (bot, message, args) => {

  var member = message.guild.members.random();

  message.channel.send(new Discord.RichEmbed()
  .setColor(randomColor())
  .addField("XP Event", `You rolled **${member.displayName}**`, true));

}

module.exports.help = {
  name: "gacha",
  anyone: true,
}