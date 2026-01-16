import { hash } from 'bcrypt';
import { User } from 'src/users/entities/user.entity';
import { MigrationInterface, QueryRunner } from 'typeorm';
import configurationsFunc from '../../config/configuration';

export class StubUsers1685095732654 implements MigrationInterface {
  // dirty hack
  private configs = configurationsFunc();

  private async hashPassword(password: string): Promise<string> {
    const passwordHash = await hash(password, this.configs.jwt.saltRounds);
    return passwordHash;
  }

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.manager
      .createQueryBuilder()
      .insert()
      .into(User)
      .values([
        {
          username: 'admin@example.com',
          passwordHash: await this.hashPassword('password'),
        },
      ])
      .execute();
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.manager
      .createQueryBuilder()
      .delete()
      .from(User)
      .where('username IN :usernames', { usernames: ['admin@example.com'] });
  }
}