import { MigrationInterface, QueryRunner } from "typeorm";

export class addWppNumberCollumn1603582396420 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE charity_events ADD COLUMN wpp_number varchar(50)`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE charity_events DROP COLUMN wpp_number`);
    }

}