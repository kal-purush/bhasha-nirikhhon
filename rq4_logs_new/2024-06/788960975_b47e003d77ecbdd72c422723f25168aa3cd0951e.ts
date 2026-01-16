import { MigrationInterface, QueryRunner } from "typeorm";

export class InitPhotos1718107194261 implements MigrationInterface {
    name = 'InitPhotos1718107194261'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "fundraiser_campaign_images" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), CONSTRAINT "PK_19dcbaa49aff0a5e87defc00c11" PRIMARY KEY ("id"))`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "amount"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "amount" double precision NOT NULL`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_first_name"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_first_name" character varying(150)`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_last_name"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_last_name" character varying(150)`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_email"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_email" character varying(255)`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_phone"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_phone" character varying(15) NOT NULL`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_city"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_city" character varying(100)`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_state"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_state" character varying(100)`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" DROP COLUMN "resolution"`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" ADD "resolution" character varying(350)`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" DROP COLUMN "money_raised_for"`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" ADD "money_raised_for" character varying(500)`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" DROP COLUMN "story"`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" ADD "story" character varying(500)`);
        await queryRunner.query(`ALTER TABLE "fundraiser" DROP COLUMN "password"`);
        await queryRunner.query(`ALTER TABLE "fundraiser" ADD "password" character varying(255)`);
        await queryRunner.query(`ALTER TABLE "fundraiser" DROP COLUMN "profile_image"`);
        await queryRunner.query(`ALTER TABLE "fundraiser" ADD "profile_image" character varying(200)`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "fundraiser" DROP COLUMN "profile_image"`);
        await queryRunner.query(`ALTER TABLE "fundraiser" ADD "profile_image" character varying`);
        await queryRunner.query(`ALTER TABLE "fundraiser" DROP COLUMN "password"`);
        await queryRunner.query(`ALTER TABLE "fundraiser" ADD "password" character varying NOT NULL`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" DROP COLUMN "story"`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" ADD "story" character varying`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" DROP COLUMN "money_raised_for"`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" ADD "money_raised_for" character varying`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" DROP COLUMN "resolution"`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" ADD "resolution" character varying`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_state"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_state" character varying`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_city"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_city" character varying`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_phone"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_phone" character varying`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_email"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_email" character varying`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_last_name"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_last_name" character varying`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "donor_first_name"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "donor_first_name" character varying`);
        await queryRunner.query(`ALTER TABLE "donation" DROP COLUMN "amount"`);
        await queryRunner.query(`ALTER TABLE "donation" ADD "amount" integer NOT NULL`);
        await queryRunner.query(`DROP TABLE "fundraiser_campaign_images"`);
    }

}