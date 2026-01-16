import * as Discord from "discord.js";
import * as ejs from 'ejs';

const levels = require('../../lib/levels.json');
const img = require('./img')

export default class leveling {

    static async levelCheck (msg:Discord.Message, xp:number) {
        for(var i = 1; i <= 20; i++) {
            if(xp == levels[i].amount) {
                if(i != 1)
                    await msg.member.roles.remove(levels[i-1].id).catch(console.error);
                await msg.member.roles.add(levels[i].id).catch(console.error);
                return leveling.imageLvl(msg, i);
            }
        }
    }

    static async imageLvl (msg:Discord.Message, level:number) {
        var avatarURL = msg.author.avatarURL({ format: 'png', dynamic: false, size: 512 })
        let cdnUrl = process.env.CDN_URL;

        var html = await ejs.renderFile('views/level.ejs', { avatarURL, level, cdnUrl });
        var file = await img.generator(808, 208, html, msg.author.tag, 'lvl')

        try {
            await msg.reply('', {files: [file]})
            if(level % 2 == 0)
                return await msg.channel.send("*hey, you've unlocked a new color, do `?color` in #commands to discover it!*")
        } catch(err) {
            console.error(err)
            return msg.reply(`You're now level ${level} ! Congrats !`)
        }
    }
}