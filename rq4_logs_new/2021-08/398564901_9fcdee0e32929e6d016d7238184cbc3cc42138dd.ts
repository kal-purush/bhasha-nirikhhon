import HttpException from 'App/Exceptions/HttpException';
import {
  Connection, getConnectionManager, getConnectionOptions,
  createConnection, getConnection, QueryRunner
} from 'typeorm';

export default class OrmMiddleware {
  public static async init() {
    try {
      let connection: Connection;
      let queryRunner: QueryRunner;

      if (!getConnectionManager().has('default')) {
        const connectionOptions = await getConnectionOptions();
        connection = await createConnection(connectionOptions);
      } else {
        connection = getConnection();
      }

      queryRunner = connection.createQueryRunner();
    } catch (error) {
      throw new HttpException('Error on database connection', 503)
    }
  }
}