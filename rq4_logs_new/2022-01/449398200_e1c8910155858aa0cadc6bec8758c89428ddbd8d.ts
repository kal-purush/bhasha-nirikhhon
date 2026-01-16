import {MigrationInterface, QueryRunner} from "typeorm";

export class controleApi1643153308263 implements MigrationInterface {
    name = 'controleApi1643153308263'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "celulares" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "imgName" character varying, "imgPath" character varying, "marca" character varying NOT NULL, "modelo" character varying NOT NULL, "imeis" character varying NOT NULL, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_cf07c71c2f2ba3dd8d818d5e4f7" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "notebooks" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "imgName" character varying, "imgPath" character varying, "marca" character varying NOT NULL, "modelo" character varying NOT NULL, "numeroPatrimonio" character varying, "numeroSerie" character varying, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_0ad71bac932e1c9761f60f742e2" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "colaboradores" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "identificacao" character varying NOT NULL, "imgName" character varying, "imgPath" character varying, "username" character varying, "password" character varying, "nome" character varying NOT NULL, "cargo" character varying, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "celularId" uuid, "notebookId" uuid, CONSTRAINT "REL_0f57e27101c6f0c12ea88b4913" UNIQUE ("celularId"), CONSTRAINT "REL_244ebe76953ad4980a493c3e24" UNIQUE ("notebookId"), CONSTRAINT "PK_43dcbea28bbd5f12859c6da8089" PRIMARY KEY ("id"))`);
        await queryRunner.query(`ALTER TABLE "colaboradores" ADD CONSTRAINT "FK_0f57e27101c6f0c12ea88b4913f" FOREIGN KEY ("celularId") REFERENCES "celulares"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "colaboradores" ADD CONSTRAINT "FK_244ebe76953ad4980a493c3e24d" FOREIGN KEY ("notebookId") REFERENCES "notebooks"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "colaboradores" DROP CONSTRAINT "FK_244ebe76953ad4980a493c3e24d"`);
        await queryRunner.query(`ALTER TABLE "colaboradores" DROP CONSTRAINT "FK_0f57e27101c6f0c12ea88b4913f"`);
        await queryRunner.query(`DROP TABLE "colaboradores"`);
        await queryRunner.query(`DROP TABLE "notebooks"`);
        await queryRunner.query(`DROP TABLE "celulares"`);
    }

}