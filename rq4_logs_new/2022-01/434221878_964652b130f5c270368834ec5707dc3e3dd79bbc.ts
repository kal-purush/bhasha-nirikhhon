// npm run db:migration:gen CreateBoard

import { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateBoard1642357739827 implements MigrationInterface {
  name = 'CreateBoard1642357739827';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            CREATE TABLE "board" (
                "boardId" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "title" character varying NOT NULL,
                "columns" text NOT NULL,
                CONSTRAINT "PK_8db634dbfdd38c51ac9aeed3b2a" PRIMARY KEY ("boardId")
            )
        `);
    await queryRunner.query(`
            ALTER TABLE "task" DROP COLUMN "boardId"
        `);
    await queryRunner.query(`
            ALTER TABLE "task"
            ADD "boardId" uuid NOT NULL
        `);
    await queryRunner.query(`
            ALTER TABLE "task"
            ADD CONSTRAINT "FK_d88edac9d7990145ff6831a7bb3" FOREIGN KEY ("boardId") REFERENCES "board"("boardId") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "task" DROP CONSTRAINT "FK_d88edac9d7990145ff6831a7bb3"
        `);
    await queryRunner.query(`
            ALTER TABLE "task" DROP COLUMN "boardId"
        `);
    await queryRunner.query(`
            ALTER TABLE "task"
            ADD "boardId" character varying NOT NULL
        `);
    await queryRunner.query(`
            DROP TABLE "board"
        `);
  }
}