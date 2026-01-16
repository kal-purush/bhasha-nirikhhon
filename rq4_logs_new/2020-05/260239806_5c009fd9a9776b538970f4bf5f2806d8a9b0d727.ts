import { Client, GuildMember, MessageEmbed } from 'discord.js';
import { MongoClient } from 'mongodb';
const url = process.env.MONGO_URL, dbName = process.env.MONGO_DBNAME;

export default async function birthdayCheck (bot:Client) {
    let today = new Date();
    let hh = today.getUTCHours()

    if(hh == 7) {
        let guild = bot.guilds.cache.find(val => val.id == process.env.GUILDID)
        let oldMembers = guild.roles.fetch(process.env.BIRTHDAYROLE);

        (await oldMembers).members.forEach(async (user:GuildMember) => {
            try {
                await user.roles.remove(process.env.BIRTHDAYROLE)
            } catch (e) {
                console.error(e)
            }
        });

        let dd = String(today.getDate()).padStart(2, '0');
        let mm = String(today.getMonth() + 1).padStart(2, '0');
        let todayString = `${mm}/${dd}`;

        let mongod = await MongoClient.connect(url, {'useUnifiedTopology': true});
        let db = mongod.db(dbName);

        let data = await db.collection('user').find({ 'birthday': { $eq: todayString } }).toArray();

        if(data.length >= 1) {
            let channel:any = guild.channels.cache.find(val => val.id == process.env.BIRTHDAYTC)

            data.forEach(async user => {
                let userInfo = await guild.members.fetch(user._id)
                userInfo.roles.add(process.env.BIRTHDAYROLE)
                const embed = new MessageEmbed();
                embed.setTitle(`**Happy Birthday, ${userInfo.user.username} ! 🎉🎉**`)
                embed.setFooter(`Born on : ${todayString}`)
                embed.setColor('#FFFF72')
                embed.setThumbnail(userInfo.user.avatarURL({ format: 'png', dynamic: false, size: 128 }))
                channel.send(`<@${user._id}>`)
                channel.send(embed)
            });
        }
    }
}