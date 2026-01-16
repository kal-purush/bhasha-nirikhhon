import { MigrationInterface, QueryRunner, TableColumn } from 'typeorm';

export class AddTypeColumnToDocumentTable1748451843456 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.addColumn(
      'document',
      new TableColumn({
        name: 'type',
        type: 'varchar',
        isNullable: false,
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropColumn('document', 'type');
  }
}