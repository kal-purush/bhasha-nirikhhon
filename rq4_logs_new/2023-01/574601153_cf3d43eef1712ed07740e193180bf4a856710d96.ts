import { MigrationInterface, QueryRunner } from 'typeorm';

export class updatePostsUnique1673680720875 implements MigrationInterface {
  name = 'updatePostsUnique1673680720875';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "posts"
            ADD CONSTRAINT "UQ_a0828eb097873f0288a434ec0cb" UNIQUE ("post_url")
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "posts" DROP CONSTRAINT "UQ_a0828eb097873f0288a434ec0cb"
        `);
  }
}