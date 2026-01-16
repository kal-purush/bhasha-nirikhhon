// npx ts-node --transpile-only ./node_modules/typeorm/cli.js migration:generate -n CreateUser -p

import { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateUser1642286615619 implements MigrationInterface {
  name = 'CreateUser1642286615619';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            CREATE TABLE "user" (
                "userId" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "name" character varying NOT NULL,
                "login" character varying NOT NULL,
                "password" character varying NOT NULL,
                CONSTRAINT "PK_d72ea127f30e21753c9e229891e" PRIMARY KEY ("userId")
            )
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            DROP TABLE "user"
        `);
  }
}