import { Pool, PoolClient } from 'pg';

export class UsersDb {
  private pgPool: Pool;

  constructor(connectionUri: string, errorHandler: (err: Error, client: PoolClient) => void) {
    this.pgPool = new Pool({ connectionString: connectionUri });
    this.pgPool.on('error', errorHandler);
  }

  public async initialize() {
    await this.pgPool.query(
      `CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        guild_id VARCHAR (20) NOT NULL,
        user_id VARCHAR (20) NOT NULL,
        exp INTEGER NOT NULL,
        level INTEGER DEFAULT 1,
        UNIQUE (guild_id, user_id)
      )`,
    );
  }

  public async getAllUsers() {
    const { rows } = await this.pgPool.query('SELECT * FROM users');
    return rows;
  }

  public async getUserRows(guildId: string, userId: string) {
    const { rows, rowCount } = await this.pgPool.query(
      'SELECT * FROM users WHERE guild_id = $1 AND user_id = $2',
      [guildId, userId],
    );
    return { rows, rowCount };
  }

  public async userExists(guildId: string, userId: string) {
    return (await this.getUserRows(guildId, userId)).rowCount;
  }

  public async increaseUserExp(amount: number, guildId: string, userId: string) {
    await this.pgPool.query(
      'UPDATE users SET exp = exp + $1 WHERE guild_id = $2 AND user_id = $3',
      [amount, guildId, userId],
    );
  }

  public async increaseUserLevel(guildId: string, userId: string) {
    await this.pgPool.query(
      'UPDATE users SET level = level + 1 WHERE guild_id = $1 AND user_id = $2',
      [guildId, userId],
    );
  }

  public async createUser(guildId: string, userId: string, defaultExp: number) {
    await this.pgPool.query('INSERT INTO users (guild_id, user_id, exp) VALUES ($1, $2, $3)', [
      guildId,
      userId,
      defaultExp,
    ]);
  }
}