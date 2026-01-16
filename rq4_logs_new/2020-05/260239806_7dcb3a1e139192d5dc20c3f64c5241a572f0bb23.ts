import { Client } from 'discord.js';
import { YouTube } from 'popyt';

const yt = new YouTube(process.env.YT_TOKEN)

export default async function subsCount (bot:Client) {
    let channel:any = bot.channels.cache.find(val => val.id == process.env.SUBCOUNT)
    let title = channel.name
    let newCount = title.replace(/\D/gim, '')

    await yt.getChannel('UC0QbcOX2gI5zruEvpSmnf6Q')
        .then(data => {
            if(newCount == data.subCount)return;
            let subs = data.subCount.toLocaleString()
            channel.edit({ name: `📊 ${subs} subs`})
        })
}