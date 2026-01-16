// npm run db:migration:gen SeedAdmin

import { MigrationInterface, QueryRunner } from 'typeorm';

import bcrypt from 'bcryptjs';

import { BCRYPT_SALT_ROUNDS } from '../../common/config';

export class SeedAdmin1642846753746 implements MigrationInterface {
  name = 'SeedAdmin1642846753746';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
        INSERT INTO "user" (name, login, password)
        VALUES (
            'admin',
            'admin',
            '${await bcrypt.hash('admin', Number(BCRYPT_SALT_ROUNDS))}'
          )
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
        DELETE FROM "user"
        WHERE login = 'admin'
    `);
  }
}

//__EOF__