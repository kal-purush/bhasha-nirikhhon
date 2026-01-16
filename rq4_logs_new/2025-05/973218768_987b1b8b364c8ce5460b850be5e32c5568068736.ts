import { MigrationInterface, QueryRunner, Table, TableColumnOptions } from 'typeorm';

export class CreateDocumentGenerationLogTable1747323381286 implements MigrationInterface {
  name = 'CreateDocumentGenerationLogTable1747323381286';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'document_generation_logs',
        columns: [
          {
            name: 'email',
            type: 'varchar',
            length: '255',
            isPrimary: true,
            isNullable: false,
          } as TableColumnOptions,
          {
            name: 'lastFreeGenerationAt',
            type: 'timestamp with time zone',
            isNullable: false,
          },
          {
            name: 'createdAt',
            type: 'timestamp with time zone',
            default: 'CURRENT_TIMESTAMP',
            isNullable: false,
          },
          {
            name: 'updatedAt',
            type: 'timestamp with time zone',
            default: 'CURRENT_TIMESTAMP',
            onUpdate: 'CURRENT_TIMESTAMP',
            isNullable: false,
          },
        ],
      }),
      true,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropTable('document_generation_logs');
  }
}