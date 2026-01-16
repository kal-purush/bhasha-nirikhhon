import { MigrationInterface, QueryRunner } from "typeorm";

export class DropOldScope1638968589735 implements MigrationInterface {
  name = "DropOldScope1638968589735";

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "emission_source" DROP COLUMN "scope_old"`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "emission_source" ADD "scope_old" character varying NOT NULL DEFAULT 'Scope 3'`
    );
  }
}