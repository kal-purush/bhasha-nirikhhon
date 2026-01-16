const Discord = require('discord.js');
const client = new Discord.Client();

client.on('ready', () => {
  console.log(`Logged in as ${client.user.tag}!`);
});

client.on('message', msg => {
  if (msg.content === 'ping') {
    msg.reply('pong');
  }
});

client.login('NDgzMTU1MDQyOTcxNDE4NjQ0.Drdz5w.rgAiXf03Byh7lS59C_HtFhtHY_U');