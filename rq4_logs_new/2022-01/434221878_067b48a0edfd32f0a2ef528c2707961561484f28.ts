// npm run db:migration:gen CreateTask

import { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateTask1642337271848 implements MigrationInterface {
  name = 'CreateTask1642337271848';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            CREATE TABLE "task" (
                "taskId" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "title" character varying NOT NULL,
                "order" integer NOT NULL,
                "boardId" character varying NOT NULL,
                "description" character varying NOT NULL,
                "userId" uuid,
                "columnId" character varying,
                CONSTRAINT "PK_c5a68aa4b5c8d38a06f8e8d4c57" PRIMARY KEY ("taskId")
            )
        `);
    await queryRunner.query(`
            ALTER TABLE "task"
            ADD CONSTRAINT "FK_f316d3fe53497d4d8a2957db8b9" FOREIGN KEY ("userId") REFERENCES "user"("userId") ON DELETE
            SET NULL ON UPDATE NO ACTION
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "task" DROP CONSTRAINT "FK_f316d3fe53497d4d8a2957db8b9"
        `);
    await queryRunner.query(`
            DROP TABLE "task"
        `);
  }
}