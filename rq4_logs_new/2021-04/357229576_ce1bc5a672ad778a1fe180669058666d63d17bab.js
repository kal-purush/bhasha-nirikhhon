const Discord = require('discord.js');

const client = new Discord.Client();

const ignore = require('./ignore.json');

const prefix = ignore.prefix;


client.once('ready', () => {
    console.log('TestBot is online.');
});

client.on('message', async message =>{
    if(!message.content.startsWith(prefix) || message.author.bot) return;

    const args = message.content.slice(prefix.length).split(/ +/);
    console.log(args);
    const command = args.shift().toLowerCase();
    var param = 60;

    if(args.length > 0)
    {
        param = args.shift().toLowerCase(); 
        console.log(param);
    }

    console.log(param);
    console.log(command);
    
    //----------|
    //Commands: |
    //----------|
    if(command === 'help')
    {
        message.channel.send('Current commands:');
        message.channel.send('[-sheesh] sheesh dog');
        message.channel.send('[-alarm] sets an one hour timer');
        message.channel.send('[-alarm ~minutes~] set a custom timer (in minutes)');
    }

    if (command === 'alarm')
    {
        message.channel.send("You have set and alarm for : " + param + " minutes");
        param = (param * 1000 * 60);

        if(message.member.voice.channel)
        {
            setTimeout(() => alarm(message.member.voice.channel), param);
        }
    }

    if (command === 'sheesh')
    {
        message.channel.send("SHEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE!!!!!!!!!!!!!");
        if(message.member.voice.channel)
        {
            sheesh(message.member.voice.channel);
        }
    }

    if(command === 'bdo')
    {
        message.channel.send("You have started an Hour! An alarm will sound every 15 minutes!");
        for(i = 0; i < 4; i++)
        {
            if(i == 0)
            {
                setTimeout(() => alarm(message.member.voice.channel), 900000);
            }
            else{
                setTimeout(() => alarm(message.member.voice.channel), 900000 - 5000);
            }
        }
    }
});

async function sheesh(voiceChannel)
{
    const connection = await voiceChannel.join();
    connection.play('sheesh.mp3', {volume: 1}).on('finish', () => {
        voiceChannel.leave();
    });
}
async function alarm(voiceChannel)
{
    const connection = await voiceChannel.join();
    connection.play('alarm.mp3', {volume: 1}).on('finish', () => {
        voiceChannel.leave();
    });
}
client.login(ignore.token);