import { MigrationInterface, QueryRunner } from "typeorm";

export class ChangeCartToOrder1743697075420 implements MigrationInterface {
    name = 'ChangeCartToOrder1743697075420'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "invoices" DROP CONSTRAINT "FK_46812348c8da544f3e1afdb712a"`);
        await queryRunner.query(`CREATE TABLE "orders" ("id" BIGSERIAL NOT NULL, "createdAt" TIMESTAMP NOT NULL DEFAULT now(), "updatedAt" TIMESTAMP NOT NULL DEFAULT now(), "userId" bigint, CONSTRAINT "PK_710e2d4957aa5878dfe94e4ac2f" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "order_items" ("id" BIGSERIAL NOT NULL, "quantity" integer NOT NULL, "createdAt" TIMESTAMP NOT NULL DEFAULT now(), "updatedAt" TIMESTAMP NOT NULL DEFAULT now(), "note" character varying NOT NULL DEFAULT '', "orderId" bigint, "pizzaId" bigint, "crustId" bigint, "extraId" bigint, "sizeId" bigint, "outerCrustId" bigint, CONSTRAINT "PK_005269d8574e6fac0493715c308" PRIMARY KEY ("id"))`);
        await queryRunner.query(`ALTER TABLE "invoices" ADD "orderId" bigint`);
        await queryRunner.query(`ALTER TABLE "invoices" DROP COLUMN "cartId"`);
        await queryRunner.query(`ALTER TABLE "invoices" ADD "cartId" integer NOT NULL`);
        await queryRunner.query(`ALTER TABLE "orders" ADD CONSTRAINT "FK_151b79a83ba240b0cb31b2302d1" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "order_items" ADD CONSTRAINT "FK_f1d359a55923bb45b057fbdab0d" FOREIGN KEY ("orderId") REFERENCES "orders"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "order_items" ADD CONSTRAINT "FK_fcddec365449d942f59bbd87a67" FOREIGN KEY ("pizzaId") REFERENCES "pizzas"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "order_items" ADD CONSTRAINT "FK_7ebb0cc2d22d4ef89c73bb0de17" FOREIGN KEY ("crustId") REFERENCES "pizza_crust"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "order_items" ADD CONSTRAINT "FK_f1d8453b9621c1b63f694051865" FOREIGN KEY ("extraId") REFERENCES "pizza_extras"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "order_items" ADD CONSTRAINT "FK_1b5d693e54d6ac67e54c2bdda78" FOREIGN KEY ("sizeId") REFERENCES "pizza_sizes"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "order_items" ADD CONSTRAINT "FK_d5b667d1225118953031b18a530" FOREIGN KEY ("outerCrustId") REFERENCES "pizza_outer_crust"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "invoices" ADD CONSTRAINT "FK_a58a78a0e0031dd93a2f56f1e8e" FOREIGN KEY ("orderId") REFERENCES "orders"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "invoices" DROP CONSTRAINT "FK_a58a78a0e0031dd93a2f56f1e8e"`);
        await queryRunner.query(`ALTER TABLE "order_items" DROP CONSTRAINT "FK_d5b667d1225118953031b18a530"`);
        await queryRunner.query(`ALTER TABLE "order_items" DROP CONSTRAINT "FK_1b5d693e54d6ac67e54c2bdda78"`);
        await queryRunner.query(`ALTER TABLE "order_items" DROP CONSTRAINT "FK_f1d8453b9621c1b63f694051865"`);
        await queryRunner.query(`ALTER TABLE "order_items" DROP CONSTRAINT "FK_7ebb0cc2d22d4ef89c73bb0de17"`);
        await queryRunner.query(`ALTER TABLE "order_items" DROP CONSTRAINT "FK_fcddec365449d942f59bbd87a67"`);
        await queryRunner.query(`ALTER TABLE "order_items" DROP CONSTRAINT "FK_f1d359a55923bb45b057fbdab0d"`);
        await queryRunner.query(`ALTER TABLE "orders" DROP CONSTRAINT "FK_151b79a83ba240b0cb31b2302d1"`);
        await queryRunner.query(`ALTER TABLE "invoices" DROP COLUMN "cartId"`);
        await queryRunner.query(`ALTER TABLE "invoices" ADD "cartId" bigint NOT NULL`);
        await queryRunner.query(`ALTER TABLE "invoices" DROP COLUMN "orderId"`);
        await queryRunner.query(`DROP TABLE "order_items"`);
        await queryRunner.query(`DROP TABLE "orders"`);
        await queryRunner.query(`ALTER TABLE "invoices" ADD CONSTRAINT "FK_46812348c8da544f3e1afdb712a" FOREIGN KEY ("cartId") REFERENCES "carts"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
    }

}