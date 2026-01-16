import { MigrationInterface, QueryRunner } from 'typeorm';

export class updateTasksTable1673880802478 implements MigrationInterface {
  name = 'updateTasksTable1673880802478';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "tasks"
            ALTER COLUMN "period"
            SET DEFAULT '30'
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "tasks"
            ALTER COLUMN "period"
            SET DEFAULT '5'
        `);
  }
}