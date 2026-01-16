const Discord = require('discord.js');
const client = new Discord.Client();
console.log("Scrpit By Dream");


client.on("ready", () => {
let channel =     client.channels.get("543433690517340290")
setInterval(function() {
channel.send(`Spam By Yacine Server Florida 4ever`);
}, 30)
})

client.login(process.env.BOT_TOKEN);