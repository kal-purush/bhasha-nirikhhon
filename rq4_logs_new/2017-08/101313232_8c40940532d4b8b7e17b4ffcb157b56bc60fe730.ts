import { Database, open } from 'sqlite';

export class DatabaseProvider {

    private db: Database = null;

    get database(): Database {
        return this.db;
    }

    initDatabase(): Promise<void> {
        // First, try to open the database:
        return open('./database.sqlite3')
            // Update db schema to the latest version using SQL-based migrations:
            .then(db => db.migrate({ force: 'last' }))
            .then(db => this.db = db)
            // return noop for void:
            .then(() => { });
    }

}