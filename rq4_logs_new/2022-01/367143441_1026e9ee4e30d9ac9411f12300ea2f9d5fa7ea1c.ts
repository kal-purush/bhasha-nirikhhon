import { MigrationInterface, QueryRunner } from "typeorm";

export class PropertyFuels1641976561226 implements MigrationInterface {
  name = "PropertyFuels1641976561226";

  public async up(queryRunner: QueryRunner): Promise<void> {
    // handle previous migration artefacts
    await queryRunner.query(
      `ALTER TYPE "calculation_result_emissionsource_enum" RENAME TO "calculation_result_emissionsource_enum_old"`
    );
    await queryRunner.query(
      `CREATE TYPE "calculation_result_emissionsource_enum" AS ENUM('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75')`
    );
    await queryRunner.query(
      `ALTER TABLE "calculation_result" ALTER COLUMN "emissionSource" TYPE "calculation_result_emissionsource_enum" USING "emissionSource"::"text"::"calculation_result_emissionsource_enum"`
    );
    await queryRunner.query(
      `DROP TYPE "calculation_result_emissionsource_enum_old"`
    );
    await queryRunner.query(
      `ALTER TYPE "data_entry_emissionsource_enum" RENAME TO "data_entry_emissionsource_enum_old"`
    );
    await queryRunner.query(
      `CREATE TYPE "data_entry_emissionsource_enum" AS ENUM('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75')`
    );
    await queryRunner.query(
      `ALTER TABLE "data_entry" ALTER COLUMN "emissionSource" TYPE "data_entry_emissionsource_enum" USING "emissionSource"::"text"::"data_entry_emissionsource_enum"`
    );
    await queryRunner.query(`DROP TYPE "data_entry_emissionsource_enum_old"`);

    // add Polttoaineet emission source to Kiinteistöhuollon polttoaineet component

    await queryRunner.query(`
            INSERT INTO component (id, name, "categoryId") VALUES (13, 'Kiinteistöhuollon polttoaineet', 1);
            INSERT INTO component_emission_sources_emission_source ("componentId", "emissionSourceId") VALUES (13, 3);        
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TYPE "data_entry_emissionsource_enum_old" AS ENUM('1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '3', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '4', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '5', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '6', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '7', '70', '71', '72', '73', '74', '75', '8', '9')`
    );
    await queryRunner.query(
      `ALTER TABLE "data_entry" ALTER COLUMN "emissionSource" TYPE "data_entry_emissionsource_enum_old" USING "emissionSource"::"text"::"data_entry_emissionsource_enum_old"`
    );
    await queryRunner.query(`DROP TYPE "data_entry_emissionsource_enum"`);
    await queryRunner.query(
      `ALTER TYPE "data_entry_emissionsource_enum_old" RENAME TO "data_entry_emissionsource_enum"`
    );
    await queryRunner.query(
      `CREATE TYPE "calculation_result_emissionsource_enum_old" AS ENUM('1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '3', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '4', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '5', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '6', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '7', '70', '71', '72', '73', '74', '75', '8', '9')`
    );
    await queryRunner.query(
      `ALTER TABLE "calculation_result" ALTER COLUMN "emissionSource" TYPE "calculation_result_emissionsource_enum_old" USING "emissionSource"::"text"::"calculation_result_emissionsource_enum_old"`
    );
    await queryRunner.query(
      `DROP TYPE "calculation_result_emissionsource_enum"`
    );
    await queryRunner.query(
      `ALTER TYPE "calculation_result_emissionsource_enum_old" RENAME TO "calculation_result_emissionsource_enum"`
    );

    // delete Kiinteistöhuollon polttoaineet component
    await queryRunner.query(`
            DELETE FROM component_emission_sources_emission_source WHERE "componentId" = 13;
            DELETE FROM component WHERE id = 13;
        `);
  }
}