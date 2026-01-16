import { MigrationInterface, QueryRunner } from 'typeorm';

export class addVinylCovers1672066678831 implements MigrationInterface {
  name = 'addVinylCovers1672066678831';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`vinyl\` ADD \`coverLarge\` varchar(255) NULL`,
    );
    await queryRunner.query(
      `ALTER TABLE \`vinyl\` ADD \`coverSmall\` varchar(255) NULL`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE \`vinyl\` DROP COLUMN \`coverSmall\``);
    await queryRunner.query(`ALTER TABLE \`vinyl\` DROP COLUMN \`coverLarge\``);
  }
}