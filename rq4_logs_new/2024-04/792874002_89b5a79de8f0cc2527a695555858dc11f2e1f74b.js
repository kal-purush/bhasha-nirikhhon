const { Client, GatewayIntentBits, Partials, Collection, DiscordAPIError} = require('discord.js');
const fs = require("fs");
const client = new Client({
	intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.GuildPresences, GatewayIntentBits.GuildMembers, GatewayIntentBits.MessageContent, GatewayIntentBits.GuildBans, GatewayIntentBits.GuildMembers, GatewayIntentBits.GuildMessageReactions, GatewayIntentBits.GuildVoiceStates, GatewayIntentBits.GuildEmojisAndStickers],
	partials: [Partials.Channel, Partials.Message, Partials.User, Partials.GuildMember, Partials.Reaction],
  autoReconnect: true
});

require('dotenv').config()

process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err);
});

client.on('error', (error) => {
  if (error instanceof DiscordAPIError) {
    console.error('Discord API error:', error);
  } else {
    console.error('Error:', error);
  }
});

client.slashCommands = new Collection();

module.exports = client;


fs.readdirSync('./handlers').forEach((handler) => {
  require(`./handlers/${handler}`)(client)
});


client.login(process.env.TOKEN)