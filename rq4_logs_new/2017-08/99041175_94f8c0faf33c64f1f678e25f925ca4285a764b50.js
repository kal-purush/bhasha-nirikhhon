const Discord = require("discord.js");
const client = new Discord.Client();
var dark = true;

client.on('ready', () => {
  console.log(`Logged in as ${client.user.tag}!`);
});

client.on('message', msg => {
  if (msg.content === 'brb') {
    msg.channel.send(msg.author + ' está AFK. @here')
  }

  if (msg.content === 'nobrb') {
    msg.channel.send(msg.author + ' no está AFK. @here')
  }

  if (msg.content === 'ayudapls') {
    msg.reply('Hay dos comandos:\nbrb: notifica que tas AFK.\nnobrb: notifica que has vuelto.')
  }
});


client.login('MjM0NDQyMDQwODQ5NTk2NDE3.DGIwDg.BOw6BeAo_nRZ03xl4qxTVJGvKik');