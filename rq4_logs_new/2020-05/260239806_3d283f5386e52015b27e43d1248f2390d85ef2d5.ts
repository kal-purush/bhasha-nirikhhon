import { MongoClient } from 'mongodb';
import { GuildMember, PartialGuildMember } from 'discord.js';
const url = process.env.MONGO_URL, dbName = process.env.MONGO_DBNAME;

export default async function memberLeave (member:GuildMember | PartialGuildMember) {
    let mongod = await MongoClient.connect(url, {'useUnifiedTopology': true});
    let db = mongod.db(dbName);

    let user = await db.collection('user').findOne({ '_id': { $eq: member.id } });
    if(user)
        await db.collection('user').updateOne({ _id: member.id }, { $set: { hidden: true }});

    return mongod.close();
}