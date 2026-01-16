import { MigrationInterface, QueryRunner } from "typeorm";

export class AddStatusToTodo1637836853959 implements MigrationInterface {
  name = "AddStatusToTodo1637836853959";

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            CREATE TYPE "public"."todo_status_enum" AS ENUM('active', 'deleted', 'done', 'archived')
        `);
    await queryRunner.query(`
            ALTER TABLE "todo"
            ADD "status" "public"."todo_status_enum" NOT NULL DEFAULT 'active'
        `);
    await queryRunner.query(`
            ALTER TABLE "todo" DROP CONSTRAINT "FK_1e982e43f63a98ad9918a86035c"
        `);
    await queryRunner.query(`
            ALTER TABLE "todo"
            ALTER COLUMN "userId"
            SET NOT NULL
        `);
    await queryRunner.query(`
            ALTER TABLE "todo"
            ADD CONSTRAINT "FK_1e982e43f63a98ad9918a86035c" FOREIGN KEY ("userId") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "todo" DROP CONSTRAINT "FK_1e982e43f63a98ad9918a86035c"
        `);
    await queryRunner.query(`
            ALTER TABLE "todo"
            ALTER COLUMN "userId" DROP NOT NULL
        `);
    await queryRunner.query(`
            ALTER TABLE "todo"
            ADD CONSTRAINT "FK_1e982e43f63a98ad9918a86035c" FOREIGN KEY ("userId") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
    await queryRunner.query(`
            ALTER TABLE "todo" DROP COLUMN "status"
        `);
    await queryRunner.query(`
            DROP TYPE "public"."todo_status_enum"
        `);
  }
}