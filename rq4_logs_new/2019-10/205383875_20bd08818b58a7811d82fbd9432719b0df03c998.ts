import { MigrationInterface, QueryRunner } from 'typeorm'

export class createTaskRun1570598990476 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<any> {
    await queryRunner.query(
      `CREATE TYPE "task_run_task_type_enum" AS ENUM('SEARCH_REFRESH', 'DETAIL_REFRESH')`,
      undefined,
    )
    await queryRunner.query(
      `CREATE TYPE "task_run_status_enum" AS ENUM('PENDING', 'RUNNING', 'SUCCESS', 'FAILURE')`,
      undefined,
    )
    await queryRunner.query(
      `CREATE TABLE "task_run" ("id" BIGSERIAL NOT NULL, "task_type" "task_run_task_type_enum" NOT NULL, "status" "task_run_status_enum" NOT NULL, "created_at" text NOT NULL, "finished_at" text NOT NULL, CONSTRAINT "PK_36326cc52f4708f36ae4e6158cc" PRIMARY KEY ("id"))`,
      undefined,
    )
  }

  public async down(queryRunner: QueryRunner): Promise<any> {
    await queryRunner.query(`DROP TABLE "task_run"`, undefined)
    await queryRunner.query(`DROP TYPE "task_run_status_enum"`, undefined)
    await queryRunner.query(`DROP TYPE "task_run_task_type_enum"`, undefined)
  }
}