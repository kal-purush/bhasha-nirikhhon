import { MongoClient, Db} from 'mongodb';

export class DbModel {
    
    database: Db = null;

    public constructor() {
        MongoClient
            .connect("mongodb://localhost:27017")
            .then( (db: Db) => this.database = db.db('dbMessagerie') )
            .catch( (reason) => console.log(reason) );
    }

    async addMessage(message: string): Promise<void> {
        await this.database.collection('messages').insertOne({ message : message });
    }

    async message(): Promise <string[]> {
      return this.database.collection('messages').find().toArray();
    }

}
