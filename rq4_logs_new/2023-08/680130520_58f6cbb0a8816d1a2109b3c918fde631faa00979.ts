import { MigrationInterface, QueryRunner } from "typeorm";

export class AddedTasks1692423574504 implements MigrationInterface {
    name = 'AddedTasks1692423574504'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "address" ("created_at" TIMESTAMP NOT NULL DEFAULT now(), "updated_at" TIMESTAMP NOT NULL DEFAULT now(), "deleted_at" TIMESTAMP, "id" SERIAL NOT NULL, "address_line1" character varying NOT NULL, "address_line2" character varying NOT NULL, "city" character varying NOT NULL, "state" character varying NOT NULL, "country" character varying NOT NULL, "pincode" character varying NOT NULL, "employee_id" uuid, CONSTRAINT "REL_7e77f562043393b08de949b804" UNIQUE ("employee_id"), CONSTRAINT "PK_d92de1f82754668b5f5f5dd4fd5" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "department" ("created_at" TIMESTAMP NOT NULL DEFAULT now(), "updated_at" TIMESTAMP NOT NULL DEFAULT now(), "deleted_at" TIMESTAMP, "id" SERIAL NOT NULL, "name" character varying NOT NULL, "description" character varying NOT NULL, CONSTRAINT "PK_9a2213262c1593bffb581e382f5" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "task_comments" ("created_at" TIMESTAMP NOT NULL DEFAULT now(), "updated_at" TIMESTAMP NOT NULL DEFAULT now(), "deleted_at" TIMESTAMP, "id" SERIAL NOT NULL, "comment" character varying NOT NULL, "url" character varying NOT NULL, "task_id" uuid, "postedBy" uuid, CONSTRAINT "PK_83b99b0b03db29d4cafcb579b77" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "task" ("created_at" TIMESTAMP NOT NULL DEFAULT now(), "updated_at" TIMESTAMP NOT NULL DEFAULT now(), "deleted_at" TIMESTAMP, "id" uuid NOT NULL DEFAULT uuid_generate_v4(), "title" character varying NOT NULL, "description" character varying NOT NULL, "deadline" date NOT NULL, "max_participants" integer NOT NULL, "task_status" character varying NOT NULL DEFAULT 'UI', "bounty" integer NOT NULL DEFAULT '0', "skills" character varying NOT NULL, "is_direct_bounty" boolean NOT NULL DEFAULT false, "createdBy" uuid, "approvedBy" uuid, CONSTRAINT "PK_fb213f79ee45060ba925ecd576e" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "employee" ("created_at" TIMESTAMP NOT NULL DEFAULT now(), "updated_at" TIMESTAMP NOT NULL DEFAULT now(), "deleted_at" TIMESTAMP, "id" uuid NOT NULL DEFAULT uuid_generate_v4(), "name" character varying NOT NULL, "email" character varying NOT NULL, "department_id" integer NOT NULL, "password" character varying NOT NULL, "role" character varying NOT NULL DEFAULT 'DEVELOPER', "joining_date" character varying NOT NULL, "status" character varying NOT NULL DEFAULT 'ACTIVE', "experience" integer NOT NULL, "bounty" integer NOT NULL DEFAULT '0', CONSTRAINT "PK_3c2bc72f03fd5abbbc5ac169498" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE UNIQUE INDEX "IDX_817d1d427138772d47eca04885" ON "employee" ("email") `);
        await queryRunner.query(`CREATE TABLE "task_employees_employee" ("task_id" uuid NOT NULL, "employee_id" uuid NOT NULL, CONSTRAINT "PK_1b389b0cad285d1a5862e292fb4" PRIMARY KEY ("task_id", "employee_id"))`);
        await queryRunner.query(`CREATE INDEX "IDX_5b83b8832ad8ef005fa8e346dd" ON "task_employees_employee" ("task_id") `);
        await queryRunner.query(`CREATE INDEX "IDX_0bb7f8a3d00bcb56f4f88cf803" ON "task_employees_employee" ("employee_id") `);
        await queryRunner.query(`ALTER TABLE "address" ADD CONSTRAINT "FK_7e77f562043393b08de949b804b" FOREIGN KEY ("employee_id") REFERENCES "employee"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "task_comments" ADD CONSTRAINT "FK_ba9e465cfc707006e60aae59946" FOREIGN KEY ("task_id") REFERENCES "task"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "task_comments" ADD CONSTRAINT "FK_aea0320c42f8f74d01f58ce6a0d" FOREIGN KEY ("postedBy") REFERENCES "employee"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "task" ADD CONSTRAINT "FK_0d5ad69a41a534dea0c786e7a6f" FOREIGN KEY ("createdBy") REFERENCES "employee"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "task" ADD CONSTRAINT "FK_97e4d78d65f9a83ec209b2f2614" FOREIGN KEY ("approvedBy") REFERENCES "employee"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "employee" ADD CONSTRAINT "FK_d62835db8c0aec1d18a5a927549" FOREIGN KEY ("department_id") REFERENCES "department"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "task_employees_employee" ADD CONSTRAINT "FK_5b83b8832ad8ef005fa8e346dd2" FOREIGN KEY ("task_id") REFERENCES "task"("id") ON DELETE CASCADE ON UPDATE CASCADE`);
        await queryRunner.query(`ALTER TABLE "task_employees_employee" ADD CONSTRAINT "FK_0bb7f8a3d00bcb56f4f88cf803f" FOREIGN KEY ("employee_id") REFERENCES "employee"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "task_employees_employee" DROP CONSTRAINT "FK_0bb7f8a3d00bcb56f4f88cf803f"`);
        await queryRunner.query(`ALTER TABLE "task_employees_employee" DROP CONSTRAINT "FK_5b83b8832ad8ef005fa8e346dd2"`);
        await queryRunner.query(`ALTER TABLE "employee" DROP CONSTRAINT "FK_d62835db8c0aec1d18a5a927549"`);
        await queryRunner.query(`ALTER TABLE "task" DROP CONSTRAINT "FK_97e4d78d65f9a83ec209b2f2614"`);
        await queryRunner.query(`ALTER TABLE "task" DROP CONSTRAINT "FK_0d5ad69a41a534dea0c786e7a6f"`);
        await queryRunner.query(`ALTER TABLE "task_comments" DROP CONSTRAINT "FK_aea0320c42f8f74d01f58ce6a0d"`);
        await queryRunner.query(`ALTER TABLE "task_comments" DROP CONSTRAINT "FK_ba9e465cfc707006e60aae59946"`);
        await queryRunner.query(`ALTER TABLE "address" DROP CONSTRAINT "FK_7e77f562043393b08de949b804b"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_0bb7f8a3d00bcb56f4f88cf803"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_5b83b8832ad8ef005fa8e346dd"`);
        await queryRunner.query(`DROP TABLE "task_employees_employee"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_817d1d427138772d47eca04885"`);
        await queryRunner.query(`DROP TABLE "employee"`);
        await queryRunner.query(`DROP TABLE "task"`);
        await queryRunner.query(`DROP TABLE "task_comments"`);
        await queryRunner.query(`DROP TABLE "department"`);
        await queryRunner.query(`DROP TABLE "address"`);
    }

}