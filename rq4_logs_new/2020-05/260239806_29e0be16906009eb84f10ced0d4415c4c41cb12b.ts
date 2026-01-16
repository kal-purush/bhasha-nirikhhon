import { MongoClient, Db } from 'mongodb';
import { Message } from 'discord.js';

import leveling from '../../js/leveling';

let cooldownMsg:stringKeyArray = [], cooldownXP:stringKeyArray = [];
interface stringKeyArray {
	[index:string]: any;
}

export default class cooldown {

    static async message (msg:Message) {
        if(!cooldownMsg[msg.author.id]) {
            cooldownMsg[msg.author.id] = 1;
            setTimeout(async () => { delete cooldownMsg[msg.author.id] }, 2500)
        } else
            cooldownMsg[msg.author.id]++;

        if(cooldownMsg[msg.author.id] == 4)
            return await msg.reply({"embed": { "title": "**Please calm down, or I'll mute you.**", "color": 13632027 }})
        else if(cooldownMsg[msg.author.id] == 6) {
            await msg.member.roles.add('636254696880734238')
            let msgReply = await msg.reply({"embed": { "title": "**You've been mute for 20 minutes. Reason : spamming.**", "color": 13632027 }})
            setTimeout(async () => {
                await msgReply.delete()
                return msg.member.roles.remove('636254696880734238')
            }, 1200000);
        }
    }

    static async exp (msg:Message, mongod:MongoClient, db:Db, date:string) {
        await db.collection('stats').updateOne({ _id: date }, { $inc: { msg: 1 } }, { upsert: true })
        if(msg.channel.id == '608630294261530624')return;

        let user = await db.collection('user').findOne({ '_id': { $eq: msg.author.id } });

        if(!user)
            await db.collection('user').insertOne({ _id: msg.author.id, exp: 1, birthday: null, fc: null, hidden: false, pat: 0, hug: 0, boop: 0, slap: 0, highfive: 0 });
        else if(!cooldownXP[msg.author.id]) {
            await db.collection('user').updateOne({ _id: msg.author.id }, { $inc: { exp: 1 }});
            leveling.levelCheck(msg, (user.exp+1));
            cooldownXP[msg.author.id] = 1;
            return setTimeout(async () => { delete cooldownXP[msg.author.id] }, 5000)
        }

        mongod.close();
    }
}