import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateAhamoveOrder1710669092619 implements MigrationInterface {
  name = 'UpdateAhamoveOrder1710669092619';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`Ahamove_Order\` ADD COLUMN \`shared_link\` VARCHAR(2048) NOT NULL AFTER \`response\``,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`Ahamove_Order\` DROP COLUMN \`shared_link\` `,
    );
  }
}