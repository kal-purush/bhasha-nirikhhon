import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateAhamoveOrder1711267137879 implements MigrationInterface {
  name = 'UpdateAhamoveOrder1711267137879';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE \`Ahamove_Order\` 
    CHANGE COLUMN \`service_id\` \`service_id\` VARCHAR(255) NULL ,
    CHANGE COLUMN \`path\` \`path\` JSON NULL ,
    CHANGE COLUMN \`requests\` \`requests\` JSON NULL ,
    CHANGE COLUMN \`payment_method\` \`payment_method\` VARCHAR(255) NULL ,
    CHANGE COLUMN \`total_pay\` \`total_pay\` INT NULL ,
    CHANGE COLUMN \`order_time\` \`order_time\` INT NULL ,
    CHANGE COLUMN \`remarks\` \`remarks\` VARCHAR(255) NULL ,
    CHANGE COLUMN \`admin_note\` \`admin_note\` VARCHAR(255) NULL ,
    CHANGE COLUMN \`route_optimized\` \`route_optimized\` TINYINT NULL ,
    CHANGE COLUMN \`idle_until\` \`idle_until\` INT NULL ,
    CHANGE COLUMN \`items\` \`items\` JSON NULL ,
    CHANGE COLUMN \`package_detail\` \`package_detail\` JSON NULL ,
    CHANGE COLUMN \`response\` \`response\` JSON NULL ,
    CHANGE COLUMN \`shared_link\` \`shared_link\` VARCHAR(2048) NULL ;`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
    `);
  }
}