import { MigrationInterface, QueryRunner } from 'typeorm';

export class createVideoTables1673269789960 implements MigrationInterface {
  name = 'createVideoTables1673269789960';

  public async up(queryRunner: QueryRunner): Promise<void> {
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
            DROP TABLE "video_channels"
        `);
    await queryRunner.query(`
            DROP TABLE "videos"
        `);
  }
}