import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddIsEmailVerifiedToUser1748344865008 implements MigrationInterface {
  name = 'AddIsEmailVerifiedToUser1748344865008';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      ALTER TABLE "user" ADD "isEmailVerified" boolean NOT NULL DEFAULT false
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      ALTER TABLE "user" DROP COLUMN "isEmailVerified"
    `);
  }
}