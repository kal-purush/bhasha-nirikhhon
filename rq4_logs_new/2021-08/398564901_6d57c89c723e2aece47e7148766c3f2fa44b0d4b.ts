import {MigrationInterface, QueryRunner} from "typeorm";

export class Establishment1629653503807 implements MigrationInterface {
    name = 'Establishment1629653503807'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "establishment" ("id" SERIAL NOT NULL, "name" character varying NOT NULL, "description" character varying NOT NULL, CONSTRAINT "PK_149bd9dc1f2bd4e825a0c474932" PRIMARY KEY ("id"))`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "establishment"`);
    }

}