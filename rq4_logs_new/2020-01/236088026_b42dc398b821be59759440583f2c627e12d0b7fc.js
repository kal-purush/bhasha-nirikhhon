const Discord = require('discord.js');
const client = new Discord.Client();
console.log("Scrpit By Dream");


client.on("ready", () => {
let channel =     client.channels.get("670384960158564386")
setInterval(function() {
channel.send(`3aber 3aber 3aber 3aber 3aber`);
}, 30)
})

client.login(process.env.BOT_TOKEN);