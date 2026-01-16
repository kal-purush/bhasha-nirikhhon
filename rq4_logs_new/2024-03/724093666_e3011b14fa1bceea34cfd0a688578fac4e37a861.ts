import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddMomoTransactions1709005940772 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE \`MomoTransaction\` (\`id\` int NOT NULL AUTO_INCREMENT, \`partnerCode\` varchar(50) NOT NULL, \`requestId\` varchar(50) NOT NULL, \`amount\` decimal(10,2) NOT NULL, \`orderId\` varchar(50) NOT NULL, \`transId\` bigint NOT NULL, \`responseTime\` bigint NOT NULL, \`orderInfo\` varchar(255) NOT NULL, \`type\` varchar(10) NOT NULL, \`resultCode\` int NOT NULL, \`redirectUrl\` varchar(255) NOT NULL, \`ipnUrl\` varchar(255) NOT NULL, \`extraData\` text NOT NULL, \`requestType\` varchar(50) NOT NULL, \`signature\` varchar(255) NOT NULL, \`lang\` varchar(2) NOT NULL DEFAULT 'en', PRIMARY KEY (\`id\`)) ENGINE=InnoDB`,
    );
    await queryRunner.query(`ALTER TABLE \`MomoTransaction\` 
        ADD COLUMN \`payUrl\` TEXT NULL AFTER \`lang\`,
        ADD COLUMN \`message\` TEXT NULL AFTER \`payUrl\`;`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TABLE \`MomoTransaction\``);
  }
}