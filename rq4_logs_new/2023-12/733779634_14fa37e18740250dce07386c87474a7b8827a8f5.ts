import { MigrationInterface, QueryRunner } from 'typeorm';

export class RemoveForeignKeyBooking1703681270332 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "booking" DROP CONSTRAINT "FK_dc9f6a94644e45d49872c1e2f10"`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      ALTER TABLE "booking"
      ADD CONSTRAINT "FK_dc9f6a94644e45d49872c1e2f10"
      FOREIGN KEY ("vehicleId")
      REFERENCES "vehicle"("id")
      ON DELETE CASCADE
    `);
  }
}