import { MigrationInterface, QueryRunner } from "typeorm";

export class Industry1643226014383 implements MigrationInterface {
  name = "Industry1643226014383";

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "registration_request" DROP CONSTRAINT "FK_3ca0e6df70839d096c53f7143b3"`
    );
    await queryRunner.query(
      `ALTER TABLE "registration_request" RENAME COLUMN "businessFieldId" TO "industryId"`
    );
    await queryRunner.query(
      `CREATE TABLE "sub_industry" ("id" SERIAL NOT NULL, "code" character varying NOT NULL, "nameFi" character varying NOT NULL, "nameEn" character varying NOT NULL, "industryId" integer, CONSTRAINT "PK_61b7f6a8ab3e956d680b2afb894" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(
      `CREATE TABLE "industry" ("id" SERIAL NOT NULL, "code" character varying NOT NULL, "nameFi" character varying NOT NULL, "nameEn" character varying NOT NULL, CONSTRAINT "PK_fc3e38485cff79e9fbba8f13831" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(
      `ALTER TABLE "sub_industry" ADD CONSTRAINT "FK_f37227aa80d6ad62f3850aa0d41" FOREIGN KEY ("industryId") REFERENCES "industry"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "registration_request" ADD CONSTRAINT "FK_70dbe5cdcc647d55ced49cf7a39" FOREIGN KEY ("industryId") REFERENCES "sub_industry"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "registration_request" DROP CONSTRAINT "FK_70dbe5cdcc647d55ced49cf7a39"`
    );
    await queryRunner.query(
      `ALTER TABLE "sub_industry" DROP CONSTRAINT "FK_f37227aa80d6ad62f3850aa0d41"`
    );
    await queryRunner.query(`DROP TABLE "industry"`);
    await queryRunner.query(`DROP TABLE "sub_industry"`);
    await queryRunner.query(
      `ALTER TABLE "registration_request" RENAME COLUMN "industryId" TO "businessFieldId"`
    );
    await queryRunner.query(
      `ALTER TABLE "registration_request" ADD CONSTRAINT "FK_3ca0e6df70839d096c53f7143b3" FOREIGN KEY ("businessFieldId") REFERENCES "business_field"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
  }
}