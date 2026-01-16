import { MigrationInterface, QueryRunner, TableColumn } from "typeorm";
import { UserTable1710430811368 } from "./1710430811368-UserTable";

export class UpdateUserTableForBot1710774583786 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.clearTable(UserTable1710430811368.TABLE_NAME);
    await queryRunner.changeColumns(UserTable1710430811368.TABLE_NAME, [
      {
        oldColumn: new TableColumn({
          name: "id",
          type: "uuid",
          isPrimary: true,
          isGenerated: true,
          generationStrategy: "uuid",
        }),
        newColumn: new TableColumn({
          name: "chatId",
          type: "int",
          isPrimary: true,
          isGenerated: false,
        }),
      },
      {
        oldColumn: new TableColumn({
          name: "email",
          type: "varchar",
          isUnique: true,
        }),
        newColumn: new TableColumn({
          name: "email",
          type: "varchar",
          isNullable: true,
          isUnique: true,
          default: "NULL",
        }),
      },
      {
        oldColumn: new TableColumn({
          name: "token",
          type: "json",
        }),
        newColumn: new TableColumn({
          name: "token",
          type: "json",
          isNullable: true,
          default: "NULL",
        }),
      },
    ]);
    await queryRunner.addColumn(
      UserTable1710430811368.TABLE_NAME,
      new TableColumn({
        name: "name",
        type: "varchar",
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.clearTable(UserTable1710430811368.TABLE_NAME);
    await queryRunner.dropColumn(UserTable1710430811368.TABLE_NAME, "name");
    await queryRunner.changeColumns(UserTable1710430811368.TABLE_NAME, [
      {
        newColumn: new TableColumn({
          name: "id",
          type: "uuid",
          isPrimary: true,
          isGenerated: true,
          generationStrategy: "uuid",
        }),
        oldColumn: new TableColumn({
          name: "chatId",
          type: "number",
          isPrimary: true,
          isGenerated: false,
        }),
      },
      {
        newColumn: new TableColumn({
          name: "email",
          type: "varchar",
          isUnique: true,
        }),
        oldColumn: new TableColumn({
          name: "email",
          type: "varchar",
          isNullable: true,
          isUnique: true,
          default: "NULL",
        }),
      },
      {
        newColumn: new TableColumn({
          name: "token",
          type: "json",
        }),
        oldColumn: new TableColumn({
          name: "token",
          type: "json",
          isNullable: true,
          default: "NULL",
        }),
      },
    ]);
  }
}