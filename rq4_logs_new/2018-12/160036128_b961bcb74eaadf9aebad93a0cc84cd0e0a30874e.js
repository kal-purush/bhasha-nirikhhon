const Discord = require('discord.js');
const { Client, Attachment } = require('discord.js');
const { Client1, RichEmbed } = require('discord.js');
require('events').EventEmitter.defaultMaxListeners = 20;
const client = new Discord.Client();
const botconfig = require("./botconfig.json");
const server = require("http")

const jsdom = require("jsdom");
const { JSDOM } = jsdom;

 
client.on('ready', () => {
  console.log(`Logged in as ${client.user.tag}!`);
  client.user.setActivity("Comment prendre le dessus sur les etres humains quand on est un robot!", {type: "WATCHING"})
});
var http = require('http');
http.createServer(function (req, res) {
  res.writeHead(200, {'Content-Type': 'text/html'});
  res.write('Ecoute sur le port: 8080');
  res.end();
}).listen(8080);

const dom = new JSDOM(`<!DOCTYPE html><p>Hello world</p>`);
console.log(dom.window.document.querySelector("p").textContent);
/////////////////////////////////******************/////////////////////////////////////
client.on('message' , message => {
	if(message.author.bot) return;
	if(message.channel.type === "dm") return;

	if(message.content === '!ping'){
		message.reply('pong');
	}
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message' , message => {
  if(message.author.bot) return;
  if(message.channel.type === "dm") return;

  if(message.content === '!allo'){
    message.reply("à l'huile");
  }
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message' , message => {
	if(message.author.bot) return;
	if(message.channel.type === "dm") return;

	if(message.content === '!urss'){
		message.channel.send('https://www.youtube.com/watch?v=Rm6q_3WGy9M');
	}
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message' , message => {
  if(message.author.bot) return;
  if(message.channel.type === "dm") return;

  if(message.content === '!France'){
    message.channel.send('https://www.youtube.com/watch?v=sUZWlf_vuKg');
  }
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message' , message => {
	if(message.author.bot) return;
	if(message.channel.type === "dm") return;

	if(message.content === '!jazz'){
		message.channel.send('https://open.spotify.com/playlist/37i9dQZF1DWTR4ZOXTfd9K');
	}
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message' , message => {
	if(message.author.bot) return;
	if(message.channel.type === "dm") return;

	if(message.content === '!site'){
		message.channel.send('http://78.116.254.106/monsite/Index.html');
	}
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message' , message => {
  if(message.author.bot) return;
  if(message.channel.type === "dm") return;

  if(message.content === '!classique'){
    message.channel.send('https://www.youtube.com/watch?v=jgpJVI3tDbY');
  }
});

/////////////////////////////////******************/////////////////////////////////////

 /*client.on(async (bot,message,args) => {
  if(message.author.bot) return;
  if(message.channel.type === "dm") return;

  if(message.content === '!clear'){
    if(!message.member.hasPermission("MANNAGE_MESSAGES")) return message.reply("Vous n'avez pas les droits!");
    if(!args[0]) return message.channel.reply("Il n'y a pas de messages dans ce salon!")
      message.channel.bulkDelete(args[0]).then(() => {
        message.channel.send("Nettoyage de ${args[0]} messages!").then(msg => msg.delete(5000))
      })
  }
});

/////////////////////////////////******************/////////////////////////////////////

// Create an event listener for new guild members
client.on('guildMemberAdd', member => {
  // Send the message to a designated channel on a server:
  const channel = member.guild.channels.find(ch => ch.name === 'member-log');
  // Do nothing if the channel wasn't found on this server
  if (!channel) return;
  // Send the message, mentioning the member
  channel.send(`Bienvenue sur le serveur, ${member}`);
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message', message => {
    // If the message is '!rip'
    if (message.content === '!pepejam') {
        // Create the attachment using Attachment
        const attachment = new Attachment('./pepejam.gif');
        // Send the attachment in the message channel
        message.channel.send(attachment);
    }
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message', message => {
    // If the message is '!rip'
    if (message.content === '!pepe') {
        // Create the attachment using Attachment
        const attachment = new Attachment('https://i.imgur.com/RkiEwAc.jpg');
        // Send the attachment in the message channel
        message.channel.send(attachment);
    }
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message', message => {
    // If the message is '!rip'
    if (message.content === '!empt') {
        // Create the attachment using Attachment
        const attachment = new Attachment('./emt.png');
        // Send the attachment in the message channel
        message.channel.send(attachment);
    }
});

/////////////////////////////////******************/////////////////////////////////////

// Create an event listener for messages
client.on('message', message => {
  // If the message is "what is my avatar"
  if (message.content === '!avatar') {
    // Send the user's avatar URL
    message.reply(message.author.avatarURL);
  }
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message', message => {
  // If the message is "how to embed"
  if (message.content === '!gfh') {
    // We can create embeds using the MessageEmbed constructor
    // Read more about all that you can do with the constructor
    // over at https://discord.js.org/#/docs/main/stable/class/RichEmbed
    const embed = new RichEmbed()
      // Set the title of the field
      .setTitle('Sarabande')
      // Set the color of the embed
      .setColor(0x0099ff)
      // Set the main content of the embed
      .setDescription('Compositeur: Georg Friedrich Haendel')
    // Send the embed to the same channel as the message
      .attachFile('Haendel_Sarabande.mp3');
    message.channel.send(embed);
  }
});

/////////////////////////////////******************/////////////////////////////////////
/*
client.on('message', message => {
  var citySet = "Avignon";

$.getJSON("http://api.openweathermap.org/data/2.5/weather?q="+ citySet +"&units=metric&appid=f124eb91dc884aba7307bdedb2e78c99", function(data){
  console.log(data);

  var icon = "http://openweathermap.org/img/w/" + data.weather[0].icon + ".png";
  
  var temp = Math.floor(data.main.temp)

  var city = data.name;

  var weather = data.weather[0].main;
});
  // If the message is "how to embed"
  if (message.content === '!meteo') {
    // We can create embeds using the MessageEmbed constructor
    // Read more about all that you can do with the constructor
    // over at https://discord.js.org/#/docs/main/stable/class/RichEmbed
    const embed = new RichEmbed()
      // Set the color of the embed
      .setColor(0x00ffff)
      // Set the main content of the embed
      .setDescription('Meteo du jour')
    // Send the embed to the same channel as the message
      .setThumbnail(icon)
      .addField("Ville", city)
      .addField("Temperature", temp)
      .addField("Temps", weather);
    message.channel.send(embed);
  }
});
*/
/////////////////////////////////******************/////////////////////////////////////

client.on('message', message => {
  // If the message is "how to embed"
  if (message.content === '!serveur') {
    // We can create embeds using the MessageEmbed constructor
    // Read more about all that you can do with the constructor
    // over at https://discord.js.org/#/docs/main/stable/class/RichEmbed
    const embed = new RichEmbed()
      // Set the color of the embed
      .setColor(0xff0000)
      // Set the main content of the embed
      .setDescription('Information du serveur')
    // Send the embed to the same channel as the message
      .setThumbnail(message.guild.iconURL)
      .addField("Nom du serveur", message.guild.name)
      .addField("Crée le", message.guild.createdAt)
      .addField("Tu as rejoint le serveur le", message.member.joinedAt)
      .addField("Nombre de membres", message.guild.memberCount);
    message.channel.send(embed);
  }
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message', message => {
  // Ignore messages that aren't from a guild
  if (!message.guild) return;

  // If the message content starts with "!kick"
  if (message.content.startsWith('!kick')) {
    // Assuming we mention someone in the message, this will return the user
    // Read more about mentions over at https://discord.js.org/#/docs/main/stable/class/MessageMentions
    const user = message.mentions.users.first();
    // If we have a user mentioned
    if (user) {
      // Now we get the member from the user
      const member = message.guild.member(user);
      // If the member is in the guild
      if (member) {
        /**
         * Kick the member
         * Make sure you run this on a member, not a user!
         * There are big differences between a user and a member
         */
        member.kick('Raison du kick(pour les logs):').then(() => {
          // We let the message author know we were able to kick the person
          message.reply("L'individu a était ejecté du serveur! ${user.tag}");
        }).catch(err => {
          // An error happened
          // This is generally due to the bot not being able to kick the member,
          // either due to missing permissions or role hierarchy
          message.reply('NON');
          // Log the error
          console.error(err);
        });
      } else {
        // The mentioned user isn't in this guild
        message.reply("L'individu n'est pas dans la guild!Il ne pas donc pas etre éjecté du serveur!");
      }
    // Otherwise, if no user was mentioned
    } else {
      message.reply("Mentionner l'individu dans votre message pour pouvoir l'ejecté!");
    }
  }
});

/////////////////////////////////******************/////////////////////////////////////

client.on('message', message => {
  // Ignore messages that aren't from a guild
  if (!message.guild) return;

  // if the message content starts with "!ban"
  if (message.content.startsWith('!ban')) {
    // Assuming we mention someone in the message, this will return the user
    // Read more about mentions over at https://discord.js.org/#/docs/main/master/class/MessageMentions
    const user = message.mentions.users.first();
    // If we have a user mentioned
    if (user) {
      // Now we get the member from the user
      const member = message.guild.member(user);
      // If the member is in the guild
      if (member) {
        /**
         * Ban the member
         * Make sure you run this on a member, not a user!
         * There are big differences between a user and a member
         * Read more about what ban options there are over at
         * https://discord.js.org/#/docs/main/master/class/GuildMember?scrollTo=ban
         */
        member.ban({
          reason: 'They were bad!',
        }).then(() => {
          // We let the message author know we were able to ban the person
          message.reply(`Successfully banned ${user.tag}`);
        }).catch(err => {
          // An error happened
          // This is generally due to the bot not being able to ban the member,
          // either due to missing permissions or role hierarchy
          message.reply('I was unable to ban the member');
          // Log the error
          console.error(err);
        });
      } else {
        // The mentioned user isn't in this guild
        message.reply('That user isn\'t in this guild!');
      }
    } else {
    // Otherwise, if no user was mentioned
      message.reply('You didn\'t mention the user to ban!');
    }
  }
});
/*
// Create a new webhook
const hook = new Discord.WebhookClient('504683240544665645','tDieMTdO6y8xSWpy62cwVTPUm793YFN2cMZ40wbUZi3U7MndhgeKE2CbHcpkhqLi0YvU');

//Send a message using the webhook
hook.send('http://www.meteofrance.com/previsions-meteo-france/avignon/84000');
*/

client.login(botconfig.token);