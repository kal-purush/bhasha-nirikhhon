import { MigrationInterface, QueryRunner } from "typeorm";

export class ChangedStatusName1692424138161 implements MigrationInterface {
    name = 'ChangedStatusName1692424138161'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "task" RENAME COLUMN "task_status" TO "status"`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "task" RENAME COLUMN "status" TO "task_status"`);
    }

}