import { MigrationInterface, QueryRunner, Table, TableForeignKey, TableIndex } from "typeorm";

export class NewMigration1703175601654 implements MigrationInterface {
  name = 'NewMigration1703175601654';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);

    const tablesToCreate = [
      {
        name: "vehicle",
        columns: [
          { name: "id", type: "serial", isPrimary: true },
          { name: "brand", type: "varchar", isNullable: false },
          { name: "model", type: "varchar", isNullable: false },
          { name: "year", type: "integer", isNullable: false },
        ],
      },
      {
        name: "booking",
        columns: [
          { name: "id", type: "serial", isPrimary: true },
          { name: "startDate", type: "timestamp", isNullable: false },
          { name: "endDate", type: "timestamp", isNullable: false },
          { name: "vehicleId", type: "integer", isNullable: true },
        ],
      },
      {
        name: "user",
        columns: [
          { name: "id", type: "serial", isPrimary: true },
          { name: "name", type: "varchar", isNullable: false },
          { name: "email", type: "varchar", isNullable: false },
          { name: "password", type: "varchar", isNullable: false },
        ],
      },
    ];

    for (const tableConfig of tablesToCreate) {
      await this.createTable(queryRunner, tableConfig);
    }
  }

  private async createTable(queryRunner: QueryRunner, config: { name: string, columns: any[] }): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: config.name,
        columns: config.columns.map(column => ({
          ...column,
          type: column.type || "varchar", // set a default type if not provided
        })),
      }),
      true,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    const tableNames = ["user", "booking", "vehicle"];
    
    for (const tableName of tableNames) {
      await queryRunner.dropTable(tableName, true);
    }
  }
}