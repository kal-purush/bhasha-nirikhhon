import { MigrationInterface, QueryRunner } from 'typeorm';

export class releaseV1Beta1673502441852 implements MigrationInterface {
  name = 'releaseV1Beta1673502441852';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            CREATE TYPE "public"."tasks_status_enum" AS ENUM('RUNNING', 'STOPPED', 'FAILED')
        `);
    await queryRunner.query(`
            CREATE TABLE "tasks" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "title" character varying NOT NULL,
                "description" character varying NOT NULL,
                "status" "public"."tasks_status_enum" NOT NULL DEFAULT 'STOPPED',
                "task_type_id" uuid,
                CONSTRAINT "PK_8d12ff38fcc62aaba2cab748772" PRIMARY KEY ("id")
            )
        `);
    await queryRunner.query(`
            CREATE TABLE "videos" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "video_id" character varying NOT NULL,
                "title" character varying NOT NULL,
                "description" character varying,
                "thumbnail_uri" character varying,
                "etag" character varying NOT NULL,
                "uploaded_at" TIMESTAMP NOT NULL,
                "created_at" TIMESTAMP NOT NULL DEFAULT now(),
                "updated_at" TIMESTAMP NOT NULL DEFAULT now(),
                "deleted_at" TIMESTAMP,
                "channel_id" uuid,
                "community_id" uuid,
                CONSTRAINT "UQ_0b143ebc78208631fad3c886ded" UNIQUE ("video_id"),
                CONSTRAINT "PK_e4c86c0cf95aff16e9fb8220f6b" PRIMARY KEY ("id")
            )
        `);
    await queryRunner.query(`
            CREATE TABLE "video_channels" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "channel_id" character varying NOT NULL,
                "title" character varying NOT NULL,
                "created_at" TIMESTAMP NOT NULL DEFAULT now(),
                "updated_at" TIMESTAMP NOT NULL DEFAULT now(),
                "deleted_at" TIMESTAMP,
                "community_id" uuid,
                CONSTRAINT "UQ_0b61161c990b685a6286647206e" UNIQUE ("channel_id"),
                CONSTRAINT "PK_5f4a03de4eaf9530c33510937e0" PRIMARY KEY ("id")
            )
        `);
    await queryRunner.query(`
            CREATE TYPE "public"."users_role_enum" AS ENUM('ADMIN', 'USER')
        `);
    await queryRunner.query(`
            CREATE TABLE "users" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "name" character varying NOT NULL,
                "email" character varying NOT NULL,
                "password" character varying NOT NULL,
                "role" "public"."users_role_enum" NOT NULL DEFAULT 'USER',
                "created_at" TIMESTAMP NOT NULL DEFAULT now(),
                "updated_at" TIMESTAMP NOT NULL DEFAULT now(),
                "deleted_at" TIMESTAMP,
                CONSTRAINT "UQ_51b8b26ac168fbe7d6f5653e6cf" UNIQUE ("name"),
                CONSTRAINT "UQ_97672ac88f789774dd47f7c8be3" UNIQUE ("email"),
                CONSTRAINT "PK_a3ffb1c0c8416b9fc6f907b7433" PRIMARY KEY ("id")
            )
        `);
    await queryRunner.query(`
            ALTER TYPE "public"."communities_title_enum"
            RENAME TO "communities_title_enum_old"
        `);
    await queryRunner.query(`
            CREATE TYPE "public"."communities_title_enum" AS ENUM('REDDIT', 'DC_INSIDE', 'TWITTER', 'YOUTUBE')
        `);
    await queryRunner.query(`
            ALTER TABLE "communities"
            ALTER COLUMN "title" TYPE "public"."communities_title_enum" USING "title"::"text"::"public"."communities_title_enum"
        `);
    await queryRunner.query(`
            DROP TYPE "public"."communities_title_enum_old"
        `);
    await queryRunner.query(`
            ALTER TABLE "tasks"
            ADD CONSTRAINT "FK_ea76a982cfc3dd4bff34daaf036" FOREIGN KEY ("task_type_id") REFERENCES "communities"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
    await queryRunner.query(`
            ALTER TABLE "videos"
            ADD CONSTRAINT "FK_023a8e4f3f1a34ff3d8ca04a4cc" FOREIGN KEY ("channel_id") REFERENCES "video_channels"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
    await queryRunner.query(`
            ALTER TABLE "videos"
            ADD CONSTRAINT "FK_ce61873a4be0f702dc9481491fb" FOREIGN KEY ("community_id") REFERENCES "communities"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
    await queryRunner.query(`
            ALTER TABLE "video_channels"
            ADD CONSTRAINT "FK_b1a37c8efcb6c1fd22e6230c779" FOREIGN KEY ("community_id") REFERENCES "communities"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "video_channels" DROP CONSTRAINT "FK_b1a37c8efcb6c1fd22e6230c779"
        `);
    await queryRunner.query(`
            ALTER TABLE "videos" DROP CONSTRAINT "FK_ce61873a4be0f702dc9481491fb"
        `);
    await queryRunner.query(`
            ALTER TABLE "videos" DROP CONSTRAINT "FK_023a8e4f3f1a34ff3d8ca04a4cc"
        `);
    await queryRunner.query(`
            ALTER TABLE "tasks" DROP CONSTRAINT "FK_ea76a982cfc3dd4bff34daaf036"
        `);
    await queryRunner.query(`
            CREATE TYPE "public"."communities_title_enum_old" AS ENUM('REDDIT', 'DC_INSIDE', 'TWITTER')
        `);
    await queryRunner.query(`
            ALTER TABLE "communities"
            ALTER COLUMN "title" TYPE "public"."communities_title_enum_old" USING "title"::"text"::"public"."communities_title_enum_old"
        `);
    await queryRunner.query(`
            DROP TYPE "public"."communities_title_enum"
        `);
    await queryRunner.query(`
            ALTER TYPE "public"."communities_title_enum_old"
            RENAME TO "communities_title_enum"
        `);
    await queryRunner.query(`
            DROP TABLE "users"
        `);
    await queryRunner.query(`
            DROP TYPE "public"."users_role_enum"
        `);
    await queryRunner.query(`
            DROP TABLE "video_channels"
        `);
    await queryRunner.query(`
            DROP TABLE "videos"
        `);
    await queryRunner.query(`
            DROP TABLE "tasks"
        `);
    await queryRunner.query(`
            DROP TYPE "public"."tasks_status_enum"
        `);
  }
}