import { MigrationInterface, QueryRunner } from "typeorm";

export class ChangeUnitPriceToDecimal1744130928070 implements MigrationInterface {
    name = 'ChangeUnitPriceToDecimal1744130928070'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "pizza_crust" ADD "price" numeric NOT NULL DEFAULT '100'`);
        await queryRunner.query(`ALTER TABLE "pizza_extras" DROP COLUMN "price"`);
        await queryRunner.query(`ALTER TABLE "pizza_extras" ADD "price" numeric NOT NULL DEFAULT '1000'`);
        await queryRunner.query(`ALTER TABLE "pizza_sizes" DROP COLUMN "price"`);
        await queryRunner.query(`ALTER TABLE "pizza_sizes" ADD "price" numeric NOT NULL DEFAULT '1000'`);
        await queryRunner.query(`ALTER TABLE "pizza_outer_crust" DROP COLUMN "price"`);
        await queryRunner.query(`ALTER TABLE "pizza_outer_crust" ADD "price" numeric NOT NULL DEFAULT '1000'`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "pizza_outer_crust" DROP COLUMN "price"`);
        await queryRunner.query(`ALTER TABLE "pizza_outer_crust" ADD "price" bigint NOT NULL DEFAULT '1000'`);
        await queryRunner.query(`ALTER TABLE "pizza_sizes" DROP COLUMN "price"`);
        await queryRunner.query(`ALTER TABLE "pizza_sizes" ADD "price" bigint NOT NULL DEFAULT '1000'`);
        await queryRunner.query(`ALTER TABLE "pizza_extras" DROP COLUMN "price"`);
        await queryRunner.query(`ALTER TABLE "pizza_extras" ADD "price" bigint NOT NULL DEFAULT '1000'`);
        await queryRunner.query(`ALTER TABLE "pizza_crust" DROP COLUMN "price"`);
    }

}