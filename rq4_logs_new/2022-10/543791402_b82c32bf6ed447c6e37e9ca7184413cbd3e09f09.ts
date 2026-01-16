import knex from "knex";
import dotenv from "dotenv";

dotenv.config();

export abstract class BaseDatabase {
  protected static connection = knex({
    client: "mysql",
    connection: {
      host: process.env.DB_HOST,
      port: 3306,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_DATABASE,
      multipleStatements: true,
    },
  });

  abstract TABLE_NAME: string;

  protected async getAll():Promise<any> {
    const result = await BaseDatabase.connection(this.TABLE_NAME).select();
    return result;
  }

  protected async create(item: any):Promise<void>  {
    await BaseDatabase.connection(this.TABLE_NAME).insert(item);
  }

  protected async getById(id: string) {
   const result = await BaseDatabase.connection(this.TABLE_NAME)
    .select()
    .where({ id}) 
    return result
  }
}

export default BaseDatabase;