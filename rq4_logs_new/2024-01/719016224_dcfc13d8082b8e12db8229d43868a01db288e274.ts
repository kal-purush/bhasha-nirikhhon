import { MigrationInterface, QueryRunner, TableColumn } from "typeorm"

export class Migrations1704208353171 implements MigrationInterface {

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.addColumn(
      'user',
      new TableColumn({
        name: 'sns_id',
        type: 'varchar',
        length: '255',
        isNullable: true,
        comment: '소셜 아이디',
      }),
    );
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "name" VARCHAR(20) NULL`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropColumn('user', 'sns_id');
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "name" VARCHAR(20) NOT NULL`);
  }
}