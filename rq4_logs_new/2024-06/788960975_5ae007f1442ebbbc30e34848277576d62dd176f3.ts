import { MigrationInterface, QueryRunner } from "typeorm";

export class New31718180749862 implements MigrationInterface {
    name = 'New31718180749862'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TYPE "public"."reset_password_otp_state_enum" AS ENUM('active', 'expired')`);
        await queryRunner.query(`CREATE TABLE "reset_password" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "email" character varying NOT NULL, "otp" character varying NOT NULL, "otp_state" "public"."reset_password_otp_state_enum" NOT NULL DEFAULT 'active', "created_at" TIMESTAMP NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone, CONSTRAINT "UQ_5b91bf6b5f71b9595c284bad2af" UNIQUE ("email"), CONSTRAINT "PK_82bffbeb85c5b426956d004a8f5" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TYPE "public"."donation_payment_type_enum" AS ENUM('offline', 'online')`);
        await queryRunner.query(`CREATE TYPE "public"."donation_payment_status_enum" AS ENUM('pending', 'success', 'failed')`);
        await queryRunner.query(`CREATE TYPE "public"."donation_certificate_enum" AS ENUM('No', 'Yes')`);
        await queryRunner.query(`CREATE TYPE "public"."donation_project_name_enum" AS ENUM('pithu', 'sehat', 'saksham', 'sashakt', 'insaniyat')`);
        await queryRunner.query(`CREATE TABLE "donation" ("donation_id" uuid NOT NULL DEFAULT uuid_generate_v4(), "donation_id_frontend" SERIAL NOT NULL, "amount" double precision NOT NULL, "donor_first_name" character varying(150), "donor_last_name" character varying(150), "pan" character varying, "donor_email" character varying(255), "donor_phone" character varying(15) NOT NULL, "donor_address" character varying, "comments" character varying, "payment_type" "public"."donation_payment_type_enum" DEFAULT 'online', "payment_status" "public"."donation_payment_status_enum" DEFAULT 'pending', "order_id" character varying, "payment_id" character varying, "payment_order_id" character varying, "payment_signature" character varying, "donation_date" date, "donor_city" character varying(100), "donor_state" character varying(100), "donor_country" character varying, "donor_bank_name" character varying, "donor_bank_branch" character varying, "donor_pincode" integer, "certificate" "public"."donation_certificate_enum" DEFAULT 'No', "reference_payment" character varying, "project_name" "public"."donation_project_name_enum", "payment_method" character varying, "created_at" TIMESTAMP NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone, "updated_at" TIMESTAMP NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone, "fundraiser_id" uuid, CONSTRAINT "PK_d4f5094f98dbbaf7ad5aea14aad" PRIMARY KEY ("donation_id"))`);
        await queryRunner.query(`CREATE TABLE "fundraiser_page" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "target_amount" integer NOT NULL DEFAULT '0', "raised_amount" integer NOT NULL DEFAULT '0', "resolution" character varying(350), "money_raised_for" character varying(500), "story" character varying(500), "supporters" text array, "gallery" text array, "created_at" TIMESTAMP NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone, "updated_at" TIMESTAMP NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone, "fundraiser_id" uuid, CONSTRAINT "REL_1b82eb961eb1157ec0072d335e" UNIQUE ("fundraiser_id"), CONSTRAINT "PK_ab6287a0d1fc3d090089d723a1e" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TYPE "public"."fundraiser_role_enum" AS ENUM('ADMIN', 'NORMAL_USER_ROLE', 'FUNDRAISER')`);
        await queryRunner.query(`CREATE TABLE "fundraiser" ("fundraiser_id" uuid NOT NULL DEFAULT uuid_generate_v4(), "f_id" SERIAL NOT NULL, "first_name" character varying NOT NULL, "last_name" character varying, "email" character varying NOT NULL, "password" character varying(255), "role" "public"."fundraiser_role_enum" NOT NULL DEFAULT 'FUNDRAISER', "status" character varying NOT NULL, "mobile_number" character varying, "address" character varying, "city" character varying, "profile_image" character varying(200), "state" character varying, "country" character varying, "pincode" integer, "dob" TIMESTAMP, "pan" character varying, "total_amount_raised" integer NOT NULL DEFAULT '0', "total_donations" integer NOT NULL DEFAULT '0', "created_at" TIMESTAMP NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone, "updated_at" TIMESTAMP NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone, CONSTRAINT "UQ_d1a6939468b5d1b53a505fd0c78" UNIQUE ("email"), CONSTRAINT "PK_ebe81045077a341c2255cfae649" PRIMARY KEY ("fundraiser_id"))`);
        await queryRunner.query(`CREATE TABLE "fundraiser_campaign_images" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), CONSTRAINT "PK_19dcbaa49aff0a5e87defc00c11" PRIMARY KEY ("id"))`);
        await queryRunner.query(`ALTER TABLE "donation" ADD CONSTRAINT "FK_0a582dfe9f85fcad8db37618e7a" FOREIGN KEY ("fundraiser_id") REFERENCES "fundraiser"("fundraiser_id") ON DELETE SET NULL ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "fundraiser_page" ADD CONSTRAINT "FK_1b82eb961eb1157ec0072d335e5" FOREIGN KEY ("fundraiser_id") REFERENCES "fundraiser"("fundraiser_id") ON DELETE CASCADE ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "fundraiser_page" DROP CONSTRAINT "FK_1b82eb961eb1157ec0072d335e5"`);
        await queryRunner.query(`ALTER TABLE "donation" DROP CONSTRAINT "FK_0a582dfe9f85fcad8db37618e7a"`);
        await queryRunner.query(`DROP TABLE "fundraiser_campaign_images"`);
        await queryRunner.query(`DROP TABLE "fundraiser"`);
        await queryRunner.query(`DROP TYPE "public"."fundraiser_role_enum"`);
        await queryRunner.query(`DROP TABLE "fundraiser_page"`);
        await queryRunner.query(`DROP TABLE "donation"`);
        await queryRunner.query(`DROP TYPE "public"."donation_project_name_enum"`);
        await queryRunner.query(`DROP TYPE "public"."donation_certificate_enum"`);
        await queryRunner.query(`DROP TYPE "public"."donation_payment_status_enum"`);
        await queryRunner.query(`DROP TYPE "public"."donation_payment_type_enum"`);
        await queryRunner.query(`DROP TABLE "reset_password"`);
        await queryRunner.query(`DROP TYPE "public"."reset_password_otp_state_enum"`);
    }

}