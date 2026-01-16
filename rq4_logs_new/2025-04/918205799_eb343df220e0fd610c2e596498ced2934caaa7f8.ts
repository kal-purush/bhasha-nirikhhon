import { MigrationInterface, QueryRunner } from "typeorm";

export class AddPizzaNameId1744901543365 implements MigrationInterface {
    name = 'AddPizzaNameId1744901543365'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "pizza_sizes" ADD "pizzaNameID" character varying NOT NULL DEFAULT 'unnamed'`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "pizza_sizes" DROP COLUMN "pizzaNameID"`);
    }

}