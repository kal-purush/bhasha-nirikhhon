import {MigrationInterface, QueryRunner} from "typeorm";

export class bookEntityAdd1646543851855 implements MigrationInterface {
    name = 'bookEntityAdd1646543851855'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "books" ("id" SERIAL NOT NULL, "name" character varying(75) NOT NULL, "description" character varying(200) NOT NULL, "status" character varying(8) NOT NULL DEFAULT 'ACTIVE', "created_at" TIMESTAMP NOT NULL DEFAULT now(), "updated_at" TIMESTAMP NOT NULL DEFAULT now(), CONSTRAINT "PK_f3f2f25a099d24e12545b70b022" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "user_books" ("user_id" integer NOT NULL, "book_id" integer NOT NULL, CONSTRAINT "PK_0277019e457282207ac281581d1" PRIMARY KEY ("user_id", "book_id"))`);
        await queryRunner.query(`CREATE INDEX "IDX_e746bb935afa81fbcaed41036f" ON "user_books" ("user_id") `);
        await queryRunner.query(`CREATE INDEX "IDX_2cf4aaa9d796a62fe330a79982" ON "user_books" ("book_id") `);
        await queryRunner.query(`ALTER TABLE "user_books" ADD CONSTRAINT "FK_e746bb935afa81fbcaed41036f1" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE`);
        await queryRunner.query(`ALTER TABLE "user_books" ADD CONSTRAINT "FK_2cf4aaa9d796a62fe330a799822" FOREIGN KEY ("book_id") REFERENCES "books"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "user_books" DROP CONSTRAINT "FK_2cf4aaa9d796a62fe330a799822"`);
        await queryRunner.query(`ALTER TABLE "user_books" DROP CONSTRAINT "FK_e746bb935afa81fbcaed41036f1"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_2cf4aaa9d796a62fe330a79982"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_e746bb935afa81fbcaed41036f"`);
        await queryRunner.query(`DROP TABLE "user_books"`);
        await queryRunner.query(`DROP TABLE "books"`);
    }

}