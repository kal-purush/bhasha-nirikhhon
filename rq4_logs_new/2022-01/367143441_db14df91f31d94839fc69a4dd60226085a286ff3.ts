import {MigrationInterface, QueryRunner} from "typeorm";

export class RegistrationRequestName1643015987657 implements MigrationInterface {
    name = 'RegistrationRequestName1643015987657'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "registration_request" ADD "orgName" character varying NOT NULL`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "registration_request" DROP COLUMN "orgName"`);
    }

}