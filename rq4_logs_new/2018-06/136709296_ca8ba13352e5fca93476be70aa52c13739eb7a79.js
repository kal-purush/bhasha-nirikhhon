const Discord = require('discord.js');

var bot = new Discord.Client();

bot.ping("ready", function() {
    bot.user.setGame("SwynBot, --help");
    console.log("Le bot a bien été connecté");
})

bot.login("NDU0OTM0NDY5Mjg4MTMyNjA4.Df0qTA.dNmQz2wzEhIyjNzJcSzFs6UbS0E");