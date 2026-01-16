import { MigrationInterface, QueryRunner } from "typeorm";

export class ChangedTaskstatusEnum1692424422059 implements MigrationInterface {
    name = 'ChangedTaskstatusEnum1692424422059'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "task" ALTER COLUMN "status" SET DEFAULT 'CREATED'`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "task" ALTER COLUMN "status" SET DEFAULT 'UI'`);
    }

}