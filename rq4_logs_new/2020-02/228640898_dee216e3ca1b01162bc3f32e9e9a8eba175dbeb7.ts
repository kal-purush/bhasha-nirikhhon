import {MigrationInterface, QueryRunner} from "typeorm";

export class createUser1580912672088 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<any> {
        await queryRunner.query('CREATE TABLE public."user" ( id character varying COLLATE pg_catalog."default" NOT NULL, name character varying COLLATE pg_catalog."default" NOT NULL, password character varying COLLATE pg_catalog."default" NOT NULL, email character varying COLLATE pg_catalog."default" NOT NULL, gender character(1) COLLATE pg_catalog."default" NOT NULL, age integer NOT NULL, CONSTRAINT "PK_cace4a159ff9f2512dd42373760" PRIMARY KEY (id)) TABLESPACE pg_default; ALTER TABLE public."user" OWNER to postgres;');
    }

    public async down(queryRunner: QueryRunner): Promise<any> {
        await queryRunner.query('DROP TABLE "user" ');
    }

}