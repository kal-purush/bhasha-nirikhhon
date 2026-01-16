import * as Discord from "discord.js";
import * as MissionLibrary from "./utils/mission";
import * as MessageHandler from "./utils/message_handler";



//*** Initialize the bot itself
const Bot = new Discord.Client({
  messageCacheLifetime: 600
});



//*** Register event handlers
Bot.on('error', console.error);
Bot.on('warn', console.warn);
Bot.on('debug', console.log);
Bot.on('disconnect', () => { console.warn('*** Disconnected!'); });
Bot.on('reconnecting', () => { console.warn('*** Reconnecting...'); });

Bot.on('ready', () => {
  console.log(`Client ready; logged in as ${Bot.user.username}#${Bot.user.discriminator} (${Bot.user.id})`);
});

Bot.on("message", (message) => {
  MessageHandler.HandleMessage(message);
});



//*** Register the commands that this bot should recognize



//*** Login to Discord and begin running the bot
Bot.login("Mjk2OTMzMTUzMDE5Mzk2MDk3.C75hLg.CtRd87FwV8tZZ8Jytu5T5VtpPAg");