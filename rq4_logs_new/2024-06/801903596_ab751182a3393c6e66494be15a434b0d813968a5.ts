import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateOrderTable1718245635828 implements MigrationInterface {
    name = 'UpdateOrderTable1718245635828';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query('ALTER TABLE `order` CHANGE `status` `status` enum (\'PENDING\', \'APPROVED\', \'READY\', \'COMPLETED\', \'REJECTED\', \'CANCELED\') NOT NULL DEFAULT \'PENDING\'');
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query('ALTER TABLE `order` CHANGE `status` `status` enum (\'PENDING\', \'APPROVED\', \'READY\', \'COMPLETED\', \'REJECTED\') NOT NULL DEFAULT \'PENDING\'');
    }

}