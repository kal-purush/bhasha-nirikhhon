import { MigrationInterface, QueryRunner, Table } from "typeorm";

export class account1637428145571 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: "account",
        columns: [
          {
            name: "id",
            type: "uuid",
            isPrimary: true,
            default: "uuid_generate_v4()",
          },
          {
            name: "name",
            type: "varchar",
            length: "180",
            isNullable: false,
          },
          {
            name: "email",
            type: "varchar",
            length: "180",
            isUnique: true,
            isNullable: false,
          },
          {
            name: "password",
            type: "varchar",
            length: "180",
            isNullable: false,
          },
          {
            name: "status",
            type: "enum",
            enum: ["active", "inactive"],
            default: `'inactive'`,
          },
          {
            name: "role",
            type: "enum",
            enum: ["user", "company"],
            default: `'user'`,
          },
          {
            name: "created_at",
            type: "timestamp with time zone",
            default: "now()",
          },
          {
            name: "updated_at",
            type: "timestamp with time zone",
            default: "now()",
          },
        ],
      })
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropTable("account");
  }
}