const Discord = require("discord.js");                      // For Embed
module.exports.run = async (bot, msg, args) => {
if(!msg.mentions.users.first()) return msg.reply("Specify atleast one User!");
msg.mentions.users.forEach(u => {
    let embed = new Discord.RichEmbed()
      .setTitle(`${u.tag}'s profile-picutre/avatar`)
      .setImage(u.displayAvatarURL);
console.log("Now sending %s",u.tag) 
    msg.channel.send(embed);
  });}
  
module.exports.help = {
    name: "avatars",
    description: "gives back avatars!",
    usage: "-",
    botowner: true
}