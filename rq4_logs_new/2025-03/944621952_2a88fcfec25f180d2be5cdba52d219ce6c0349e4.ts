import { db } from '../../db/databaseClient';
import { UserDto } from './Users.dto';
import { checkTableExists, dropTableIfExist } from '../../db/CommonQueries';

export class UserDao {
    static tableName = 'users';

    static async createTable(): Promise<void> {
        await db.query(`
        CREATE TABLE ${this.tableName} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT CHECK (age BETWEEN 0 AND 130) NOT NULL,
            birth TIMESTAMP NOT NULL
        ) 
    `);
    }

    static async dropTableIfExist() {
        return dropTableIfExist(this.tableName);
    }

    static async checkTableExists() {
        return checkTableExists(this.tableName);
    }

    static async insertMockData(mok: Omit<UserDto, 'id'>[]) {
        const users = [
            { name: 'Alice', age: 25 },
            { name: 'Bob', age: 40 },
            { name: 'Charlie', age: 32 },
            { name: 'David', age: 50 },
            { name: 'Eve', age: 22 },
        ];

        for (const el of mok) {
            await db.query('INSERT INTO users (name, age) VALUES (@name, @age)', { name: el.name, age: el.age });
        }
    }

    static async selectAll(): Promise<UserDto[]> {
        const { recordset } = await db.query(`SELECT * FROM ${this.tableName}`);

        return recordset.map((recordset) => new UserDto(recordset));
    }

    static async selectById(id: number) {
        const { recordset } = await db.query('SELECT * FROM Users WHERE id = @id', { id });
        return recordset.map((recordset) => new UserDto(recordset));
    }
}