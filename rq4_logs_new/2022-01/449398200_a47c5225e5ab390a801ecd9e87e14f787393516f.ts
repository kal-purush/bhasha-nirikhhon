import {MigrationInterface, QueryRunner} from "typeorm";

export class controleApiMigrations1642537888655 implements MigrationInterface {
    name = 'controleApiMigrations1642537888655'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "colaboradores" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "identificacao" character varying NOT NULL, "nome" character varying NOT NULL, "cargo" character varying, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_43dcbea28bbd5f12859c6da8089" PRIMARY KEY ("id"))`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "colaboradores"`);
    }

}