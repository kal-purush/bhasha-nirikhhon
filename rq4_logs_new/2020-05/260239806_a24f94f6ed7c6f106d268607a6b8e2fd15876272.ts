import { Client, Message } from 'discord.js'
import { Db } from 'mongodb'
const tronky = require('./tronky');

module.exports.run = async (bot:Client, msg:Message, args:string[]) => {
    tronky.run(bot, msg, args);
};

module.exports.help = {
    name: 'givetronky',
    usage: "?givetronky (mention someone)",
    desc: "Give people tronkys. Alias of `?tronky`."
};