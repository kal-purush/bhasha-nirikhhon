import { MongoClient } from 'mongodb';
import { MessageReaction, User, MessageEmbed, Client } from 'discord.js';
const url = process.env.MONGO_URL, dbName = process.env.MONGO_DBNAME;

export default async function highfiveWatcher (reaction:MessageReaction, author:User, bot:Client) {
    if(reaction.emoji.name === '✋') {
        let mongod = await MongoClient.connect(url, {'useUnifiedTopology': true});
        let db = mongod.db(dbName);
        let result = await db.collection('highfive').findOne({ '_id': { $eq: reaction.message.id }, 'target': { $eq: author.id } });

        if(result) {
            await reaction.message.delete()

            const embed = new MessageEmbed();
            embed.setColor('#F2DEB0')
            embed.setDescription(`**<@${result.author}> 🙌 <@${author.id}>**`)
            embed.setImage(`https://${process.env.CDN_URL}/img/highfive/${result.gif}.gif`)
            await db.collection('highfive').deleteOne({ 'target': { $eq: author.id } })

            await db.collection('user').updateOne({ '_id': { $eq: result.author } }, { $inc: { highfive: 1 }});
            await db.collection('user').updateOne({ '_id': { $eq: author.id } }, { $inc: { highfive: 1 }});
            await db.collection('user').updateOne({ '_id': { $eq: bot.user.id } }, { $inc: { highfive: 1 }});
            return reaction.message.channel.send(embed)
            .then(() => {
                console.log(`info: highfive`);
            })
            .catch(console.error);
        }
    }
}