import { MigrationInterface, QueryRunner } from 'typeorm';

export class InitialMigration1613296291562 implements MigrationInterface {
  public name = 'InitialMigration1613296291562';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            CREATE TABLE "dump_metadata" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "name" character varying(100) NOT NULL,
                "bucket" character varying(63) NOT NULL,
                "timestamp" TIMESTAMP NOT NULL,
                "description" character varying(256),
                CONSTRAINT "PK_8184d4a89d0e62adb70bcc0f37e" PRIMARY KEY ("id")
            )
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            DROP TABLE "dump_metadata"
        `);
  }
}