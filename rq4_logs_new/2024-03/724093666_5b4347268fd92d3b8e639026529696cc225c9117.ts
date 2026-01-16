import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateMomoTransactionTable1710911376008
  implements MigrationInterface
{
  name = 'UpdateMomoTransactionTable1710911376008';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`MomoTransaction\` 
      CHANGE COLUMN \`partnerCode\` \`partnerCode\` VARCHAR(50) NULL ,
      CHANGE COLUMN \`requestId\` \`requestId\` VARCHAR(50) NULL ,
      CHANGE COLUMN \`amount\` \`amount\` DECIMAL(10,2) NULL ,
      CHANGE COLUMN \`orderId\` \`orderId\` VARCHAR(50) NULL ,
      CHANGE COLUMN \`transId\` \`transId\` BIGINT NULL ,
      CHANGE COLUMN \`responseTime\` \`responseTime\` BIGINT NULL ,
      CHANGE COLUMN \`orderInfo\` \`orderInfo\` VARCHAR(255) NULL ,
      CHANGE COLUMN \`type\` \`type\` VARCHAR(10) NULL ,
      CHANGE COLUMN \`resultCode\` \`resultCode\` INT NULL ,
      CHANGE COLUMN \`redirectUrl\` \`redirectUrl\` VARCHAR(255) NULL ,
      CHANGE COLUMN \`ipnUrl\` \`ipnUrl\` VARCHAR(255) NULL ,
      CHANGE COLUMN \`extraData\` \`extraData\` TEXT NULL ,
      CHANGE COLUMN \`requestType\` \`requestType\` VARCHAR(50) NULL ,
      CHANGE COLUMN \`signature\` \`signature\` VARCHAR(255) NULL ,
      CHANGE COLUMN \`lang\` \`lang\` VARCHAR(2) NULL DEFAULT 'en' ;
      `,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DELETE FROM \`MomoTransaction\` where id > 0`);
    await queryRunner.query(
      `ALTER TABLE \`MomoTransaction\` 
      CHANGE COLUMN \`partnerCode\` \`partnerCode\` VARCHAR(50) NOT NULL ,
      CHANGE COLUMN \`requestId\` \`requestId\` VARCHAR(50) NOT NULL ,
      CHANGE COLUMN \`amount\` \`amount\` DECIMAL(10,2) NOT NULL ,
      CHANGE COLUMN \`orderId\` \`orderId\` VARCHAR(50) NOT NULL ,
      CHANGE COLUMN \`transId\` \`transId\` BIGINT NOT NULL ,
      CHANGE COLUMN \`responseTime\` \`responseTime\` BIGINT NOT NULL ,
      CHANGE COLUMN \`orderInfo\` \`orderInfo\` VARCHAR(255) NOT NULL ,
      CHANGE COLUMN \`type\` \`type\` VARCHAR(10) NOT NULL ,
      CHANGE COLUMN \`resultCode\` \`resultCode\` INT NOT NULL ,
      CHANGE COLUMN \`redirectUrl\` \`redirectUrl\` VARCHAR(255) NOT NULL ,
      CHANGE COLUMN \`ipnUrl\` \`ipnUrl\` VARCHAR(255) NOT NULL ,
      CHANGE COLUMN \`extraData\` \`extraData\` TEXT NOT NULL ,
      CHANGE COLUMN \`requestType\` \`requestType\` VARCHAR(50) NOT NULL ,
      CHANGE COLUMN \`signature\` \`signature\` VARCHAR(255) NOT NULL ,
      CHANGE COLUMN \`lang\` \`lang\` VARCHAR(2) NOT NULL DEFAULT 'en' ;
      `,
    );
  }
}