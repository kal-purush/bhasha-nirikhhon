import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateOrderTable1718608669819 implements MigrationInterface {
    name = 'UpdateOrderTable1718608669819';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query('ALTER TABLE `order` ADD `phoneNumber` varchar(30) NULL');
        await queryRunner.query('ALTER TABLE `order` CHANGE `status` `status` enum (\'PENDING\', \'APPROVED\', \'READY\', \'DELIVERED\', \'COMPLETED\', \'REJECTED\', \'CANCELED\') NOT NULL DEFAULT \'PENDING\'');
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query('ALTER TABLE `order` CHANGE `status` `status` enum (\'PENDING\', \'APPROVED\', \'READY\', \'COMPLETED\', \'REJECTED\', \'CANCELED\') NOT NULL DEFAULT \'PENDING\'');
        await queryRunner.query('ALTER TABLE `order` DROP COLUMN `phoneNumber`');
    }

}