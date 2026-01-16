const { Client, Attachment, Discord } = require('discord.js');
const client = new Client();

const config = require('./config.json')

client.on('ready', () => {
    console.log('I am ready!');
});

client.on('message', message => {



    // Here we separate our "command" name, and our "arguments" for the command. 
    // e.g. if we have the message "+say Is this the real life?" , we'll get the following:
    // command = say
    // args = ["Is", "this", "the", "real", "life?"]

    const args = message.content.slice(config.prefix.length).trim().split(/ +/g);
    const command = args.shift().toLowerCase();

    if (command === "ping") {
        // Calculates ping between sending a message and editing it, giving a nice round-trip latency.
        // The second ping is an average latency between the bot and the websocket server (one-way, not round-trip)
        const m = message.channel.send("Ping?");
        m.edit(`Pong! Latency is ${m.createdTimestamp - message.createdTimestamp}ms. API Latency is ${Math.round(client.ping)}ms`);
    }
});

client.on('message', message => {
    if (message.content === "what is my avatar") {
        message.reply(message.author.avatarURL)
    }
});

client.on('message', message => {
    if (message.content === 'respekt') {
        const attachment = new Attachment('https://karrierebibel.de/wp-content/uploads/2017/03/Respekt-verschaffen-Arbeit-Respekt-Definition.png');
        message.channel.send(`${message.author},`, attachment)
    }
});

client.on('message', message => {
    if (!message.guild) return;

    if (message.content.startsWith('!kick')) {
        const user = message.mentions.users.first();
        if (user) {
            const member = message.guild.member(user);
            if (member) {
                member.kick('Optional reason that will display in the audit logs').then(() => {
                    message.reply(`${user.tag} e honger 1 shkelm prej ${message.author}`);
                }).catch(err => {
                    message.reply("Unable to perform that!");
                    console.error(err);
                });
            } else {
                message.reply("Couldnt find user");
            }
        } else {
            message.reply('You didn\'t mention a user')
        }
    }
});

client.login(config.token)