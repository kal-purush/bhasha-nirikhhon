import { MigrationInterface, QueryRunner } from 'typeorm';

export class createUser1560522310253 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<any> {
    await queryRunner.query(
      `CREATE TABLE "public"."user" ("id" uuid NOT NULL, "email" character varying(255) NOT NULL, "password" text NOT NULL, CONSTRAINT "PK_03b91d2b8321aa7ba32257dc321" PRIMARY KEY ("id"))`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<any> {
    await queryRunner.query(`DROP TABLE "public"."user"`);
  }
}