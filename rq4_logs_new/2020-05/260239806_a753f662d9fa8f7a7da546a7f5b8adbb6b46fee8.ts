import { MessageReaction, User } from "discord.js";
import { MongoClient } from 'mongodb';
const url = process.env.MONGO_URL, dbName = process.env.MONGO_DBNAME;

export default class reactionRoles {

    static async add (reaction:MessageReaction, author:User) {
        let mongod = await MongoClient.connect(url, {'useUnifiedTopology': true});
        let db = mongod.db(dbName);

        let msg = await db.collection('msg').findOne({ _id: reaction.message.id })
        if(!msg)return mongod.close();

        let role = msg.roles.find((val:any) => val.emote == reaction.emoji.name)
        if(!role)return mongod.close();

        let member = reaction.message.guild.member(author)
        if(!member)return mongod.close();
        await member.roles.add(role.id)

        return mongod.close();
    }

    static async remove (reaction:MessageReaction, author:User) {
        let mongod = await MongoClient.connect(url, {'useUnifiedTopology': true});
        let db = mongod.db(dbName);

        let msg = await db.collection('msg').findOne({ _id: reaction.message.id })
        if(!msg)return mongod.close();

        let role = msg.roles.find((val:any) => val.emote == reaction.emoji.name)
        if(!role)return mongod.close();

        let member = reaction.message.guild.member(author)
        if(!member)return mongod.close();
        await member.roles.remove(role.id)

        return mongod.close();
    }
}