import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddDocumentTypes1746880996424 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      INSERT INTO document_type (id, name, description, "createdAt", "updatedAt")
      VALUES 
      (gen_random_uuid(), 'power-of-attorney', 'Power of Attorney', now(), now())
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      DELETE FROM document_type WHERE name = 'power-of-attorney'
    `);
  }
}