import { MigrationInterface, QueryRunner, Table } from 'typeorm';

export class CreateUserTable1685050336357 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'user',
        columns: [
          {
            name: 'id',
            type: 'UUID',
            isPrimary: true,
          },
          {
            name: 'name',
            type: 'varchar(255)',
            isNullable: false,
            isUnique: true,
          },
          {
            name: 'passwordHash',
            type: 'text',
            isNullable: false,
          },
        ],
      }),
      true,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropTable('user');
  }
}