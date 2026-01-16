import { MigrationInterface, QueryRunner } from 'typeorm';

export class updateUsersTable1673716712828 implements MigrationInterface {
  name = 'updateUsersTable1673716712828';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "users" DROP CONSTRAINT "UQ_51b8b26ac168fbe7d6f5653e6cf"
        `);
    await queryRunner.query(`
            ALTER TABLE "users" DROP COLUMN "name"
           
        `);
    await queryRunner.query(`
            ALTER TABLE "users"
            ADD "username" character varying NOT NULL
        `);
    await queryRunner.query(`
            ALTER TABLE "users"
            ADD CONSTRAINT "UQ_fe0bb3f6520ee0469504521e710" UNIQUE ("username")
        `);
    await queryRunner.query(`
            CREATE TYPE "public"."users_o_auth_type_enum" AS ENUM('KAKAO', 'GOOGLE')
        `);
    await queryRunner.query(`
            ALTER TABLE "users"
            ADD "o_auth_type" "public"."users_o_auth_type_enum"
        `);
    await queryRunner.query(`
            ALTER TABLE "posts" DROP CONSTRAINT "UQ_a0828eb097873f0288a434ec0cb"
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "posts"
            ADD CONSTRAINT "UQ_a0828eb097873f0288a434ec0cb" UNIQUE ("post_url")
        `);
    await queryRunner.query(`
            ALTER TABLE "users" DROP COLUMN "o_auth_type"
        `);
    await queryRunner.query(`
            DROP TYPE "public"."users_o_auth_type_enum"
        `);
    await queryRunner.query(`
            ALTER TABLE "users" DROP CONSTRAINT "UQ_fe0bb3f6520ee0469504521e710"
        `);
    await queryRunner.query(`
            ALTER TABLE "users" DROP COLUMN "username"
        `);
    await queryRunner.query(`
            ALTER TABLE "users"
            ADD "name" character varying NOT NULL
        `);
    await queryRunner.query(`
            ALTER TABLE "users"
            ADD CONSTRAINT "UQ_51b8b26ac168fbe7d6f5653e6cf" UNIQUE ("name")
        `);
  }
}