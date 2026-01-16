const graf = require('discord-graf');
const storage = require('../../index').Storage;
const Player = require('../../player');
const DiscordUser = require('../../discordUser');

module.exports = class RegisterCommand extends graf.Command {
    constructor(bot) {
        super(bot, {
            name: 'register',
            module: 'discordscape',
            memberName: 'register',
            description: 'Create account for DiscordScape as a Discord User.',
            usage: 'register',
            details: 'Links whichever Discord User sends this command to a new DiscordScape account if they don\'t already have one',
            examples: ['register']
        });
    }

    run(message, args) {
        const author = message.author;
        const user = new DiscordUser(author.id, author.username, author.discriminator);
        console.log(`Discord user: "${user.getDiscordUsername()}" executed register command`);
        // Store player with ID as unique discord username
        storage.setItem(user.getID(), new Player(user));
        return Promise.resolve(`Adventurer ${user.getName()} has entered the world of DiscordScape!`);
    }
};