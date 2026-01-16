import { MigrationInterface, QueryRunner } from "typeorm";

export class UpdateOrderTable1718258808118 implements MigrationInterface {
    name = 'UpdateOrderTable1718258808118'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE \`order\` ADD \`rejectReason\` varchar(1000) NULL`);
        await queryRunner.query(`ALTER TABLE \`order\` CHANGE \`status\` \`status\` enum ('PENDING', 'APPROVED', 'READY', 'COMPLETED', 'REJECTED', 'CANCELED') NOT NULL DEFAULT 'PENDING'`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE \`order\` CHANGE \`status\` \`status\` enum ('PENDING', 'APPROVED', 'READY', 'COMPLETED', 'REJECTED') NOT NULL DEFAULT 'PENDING'`);
        await queryRunner.query(`ALTER TABLE \`order\` DROP COLUMN \`rejectReason\``);
    }

}