// npm run db:migration:gen CreateBoardColumn

import { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateBoardColumn1642422281489 implements MigrationInterface {
  name = 'CreateBoardColumn1642422281489';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            CREATE TABLE "board_column" (
                "columnId" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "title" character varying NOT NULL,
                "order" integer NOT NULL,
                "boardId" uuid,
                CONSTRAINT "PK_b4998ae27b61758fe4e19f2de00" PRIMARY KEY ("columnId")
            )
        `);
    await queryRunner.query(`
            ALTER TABLE "board" DROP COLUMN "columns"
        `);
    await queryRunner.query(`
            ALTER TABLE "board_column"
            ADD CONSTRAINT "FK_7d6b58efcc37a760ffd108eec72" FOREIGN KEY ("boardId") REFERENCES "board"("boardId") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "board_column" DROP CONSTRAINT "FK_7d6b58efcc37a760ffd108eec72"
        `);
    await queryRunner.query(`
            ALTER TABLE "board"
            ADD "columns" text NOT NULL
        `);
    await queryRunner.query(`
            DROP TABLE "board_column"
        `);
  }
}