import { MigrationInterface, QueryRunner } from "typeorm";

export class CreateTables1695567607600 implements MigrationInterface {
    name = 'CreateTables1695567607600'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "message" ("id" SERIAL NOT NULL, "senderId" integer NOT NULL, "content" text NOT NULL, "sent_at" date NOT NULL DEFAULT now(), "timestamp" TIMESTAMP NOT NULL, "deletedAt" date, "chatId" integer, CONSTRAINT "PK_ba01f0a3e0123651915008bc578" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "profileInfos" ("id" SERIAL NOT NULL, "description" text, "numberFollowers" integer NOT NULL DEFAULT '0', "numberFollowing" integer NOT NULL DEFAULT '0', "numberPosts" integer NOT NULL DEFAULT '0', "createdAt" date NOT NULL DEFAULT now(), "updatedAt" date NOT NULL DEFAULT now(), "deletedAt" date, "userId" integer, CONSTRAINT "REL_e514ff7e2d51bcbfdfddfcd72b" UNIQUE ("userId"), CONSTRAINT "PK_e908c6c2a8f5aac99f48490949a" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "posts" ("id" SERIAL NOT NULL, "numberShares" integer NOT NULL DEFAULT '0', "description" text, "imgs" text, "numLikes" integer NOT NULL DEFAULT '0', "numComments" integer NOT NULL DEFAULT '0', "createdAt" date NOT NULL DEFAULT now(), "updatedAt" date NOT NULL DEFAULT now(), "deletedAt" date, "userId" integer, CONSTRAINT "PK_2829ac61eff60fcec60d7274b9e" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "likes" ("id" SERIAL NOT NULL, "createdAt" date NOT NULL DEFAULT now(), "updatedAt" date NOT NULL DEFAULT now(), "deletedAt" date, "userId" integer, "postId" integer NOT NULL, CONSTRAINT "PK_a9323de3f8bced7539a794b4a37" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "users" ("id" SERIAL NOT NULL, "fullName" character varying(60) NOT NULL, "nickname" character varying(20) NOT NULL, "email" character varying(45) NOT NULL, "phone" character varying(20), "profileImg" text NOT NULL DEFAULT '', "bannerImg" text NOT NULL DEFAULT '', "banned" boolean NOT NULL DEFAULT false, "suspended" boolean NOT NULL DEFAULT false, "suspendedTime" TIME, "admin" boolean NOT NULL DEFAULT false, "verified" boolean NOT NULL DEFAULT false, "confirmed" boolean NOT NULL DEFAULT false, "password" character varying(120) NOT NULL, "createdAt" date NOT NULL DEFAULT now(), "updatedAt" date NOT NULL DEFAULT now(), "deletedAt" date, "profileInfosId" integer, CONSTRAINT "UQ_ad02a1be8707004cb805a4b5023" UNIQUE ("nickname"), CONSTRAINT "UQ_97672ac88f789774dd47f7c8be3" UNIQUE ("email"), CONSTRAINT "UQ_a000cca60bcf04454e727699490" UNIQUE ("phone"), CONSTRAINT "REL_f7297f2289efb670fb2348bfa8" UNIQUE ("profileInfosId"), CONSTRAINT "PK_a3ffb1c0c8416b9fc6f907b7433" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "chat" ("id" SERIAL NOT NULL, "roomId" character varying NOT NULL, CONSTRAINT "PK_9d0b2ba74336710fd31154738a5" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "comments" ("id" SERIAL NOT NULL, "description" character varying(255), "images" character varying array NOT NULL, "createdAt" date NOT NULL DEFAULT now(), "deletedAt" date, "userId" integer, "postId" integer, CONSTRAINT "REL_7e8d7c49f218ebb14314fdb374" UNIQUE ("userId"), CONSTRAINT "REL_e44ddaaa6d058cb4092f83ad61" UNIQUE ("postId"), CONSTRAINT "PK_8bf68bc960f2b69e818bdb90dcb" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "users_chats_chat" ("usersId" integer NOT NULL, "chatId" integer NOT NULL, CONSTRAINT "PK_1ea105a595d621c3df0b7d7c01c" PRIMARY KEY ("usersId", "chatId"))`);
        await queryRunner.query(`CREATE INDEX "IDX_5937b033223a9f80a2338efe42" ON "users_chats_chat" ("usersId") `);
        await queryRunner.query(`CREATE INDEX "IDX_b0c01a6065a43f9bb1bcf460a9" ON "users_chats_chat" ("chatId") `);
        await queryRunner.query(`CREATE TABLE "users_followers_users" ("usersId_1" integer NOT NULL, "usersId_2" integer NOT NULL, CONSTRAINT "PK_ee8a9c5a097f32b484caaeb3de7" PRIMARY KEY ("usersId_1", "usersId_2"))`);
        await queryRunner.query(`CREATE INDEX "IDX_8d63f6043394b4d32343bdea11" ON "users_followers_users" ("usersId_1") `);
        await queryRunner.query(`CREATE INDEX "IDX_1433e3275a501bc09f5c33c7ca" ON "users_followers_users" ("usersId_2") `);
        await queryRunner.query(`CREATE TABLE "users_following_users" ("usersId_1" integer NOT NULL, "usersId_2" integer NOT NULL, CONSTRAINT "PK_a41e4dcaab91b51d41267c083ea" PRIMARY KEY ("usersId_1", "usersId_2"))`);
        await queryRunner.query(`CREATE INDEX "IDX_f257a3163914a2e398d7bfa800" ON "users_following_users" ("usersId_1") `);
        await queryRunner.query(`CREATE INDEX "IDX_4a5ef04f52de98c49916c8798e" ON "users_following_users" ("usersId_2") `);
        await queryRunner.query(`CREATE TABLE "chat_users_users" ("chatId" integer NOT NULL, "usersId" integer NOT NULL, CONSTRAINT "PK_713b86e11c5953a582db371fa26" PRIMARY KEY ("chatId", "usersId"))`);
        await queryRunner.query(`CREATE INDEX "IDX_cba23e8b1a61ac2f4b84060c57" ON "chat_users_users" ("chatId") `);
        await queryRunner.query(`CREATE INDEX "IDX_dbcc3f102974a9e7213c35edac" ON "chat_users_users" ("usersId") `);
        await queryRunner.query(`ALTER TABLE "message" ADD CONSTRAINT "FK_619bc7b78eba833d2044153bacc" FOREIGN KEY ("chatId") REFERENCES "chat"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "profileInfos" ADD CONSTRAINT "FK_e514ff7e2d51bcbfdfddfcd72b7" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "posts" ADD CONSTRAINT "FK_ae05faaa55c866130abef6e1fee" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "likes" ADD CONSTRAINT "FK_cfd8e81fac09d7339a32e57d904" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "likes" ADD CONSTRAINT "FK_e2fe567ad8d305fefc918d44f50" FOREIGN KEY ("postId") REFERENCES "posts"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "users" ADD CONSTRAINT "FK_f7297f2289efb670fb2348bfa81" FOREIGN KEY ("profileInfosId") REFERENCES "profileInfos"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "comments" ADD CONSTRAINT "FK_7e8d7c49f218ebb14314fdb3749" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "comments" ADD CONSTRAINT "FK_e44ddaaa6d058cb4092f83ad61f" FOREIGN KEY ("postId") REFERENCES "posts"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "users_chats_chat" ADD CONSTRAINT "FK_5937b033223a9f80a2338efe426" FOREIGN KEY ("usersId") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE`);
        await queryRunner.query(`ALTER TABLE "users_chats_chat" ADD CONSTRAINT "FK_b0c01a6065a43f9bb1bcf460a97" FOREIGN KEY ("chatId") REFERENCES "chat"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "users_followers_users" ADD CONSTRAINT "FK_8d63f6043394b4d32343bdea11d" FOREIGN KEY ("usersId_1") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE`);
        await queryRunner.query(`ALTER TABLE "users_followers_users" ADD CONSTRAINT "FK_1433e3275a501bc09f5c33c7ca2" FOREIGN KEY ("usersId_2") REFERENCES "users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "users_following_users" ADD CONSTRAINT "FK_f257a3163914a2e398d7bfa8001" FOREIGN KEY ("usersId_1") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE`);
        await queryRunner.query(`ALTER TABLE "users_following_users" ADD CONSTRAINT "FK_4a5ef04f52de98c49916c8798ec" FOREIGN KEY ("usersId_2") REFERENCES "users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "chat_users_users" ADD CONSTRAINT "FK_cba23e8b1a61ac2f4b84060c573" FOREIGN KEY ("chatId") REFERENCES "chat"("id") ON DELETE CASCADE ON UPDATE CASCADE`);
        await queryRunner.query(`ALTER TABLE "chat_users_users" ADD CONSTRAINT "FK_dbcc3f102974a9e7213c35edacc" FOREIGN KEY ("usersId") REFERENCES "users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "chat_users_users" DROP CONSTRAINT "FK_dbcc3f102974a9e7213c35edacc"`);
        await queryRunner.query(`ALTER TABLE "chat_users_users" DROP CONSTRAINT "FK_cba23e8b1a61ac2f4b84060c573"`);
        await queryRunner.query(`ALTER TABLE "users_following_users" DROP CONSTRAINT "FK_4a5ef04f52de98c49916c8798ec"`);
        await queryRunner.query(`ALTER TABLE "users_following_users" DROP CONSTRAINT "FK_f257a3163914a2e398d7bfa8001"`);
        await queryRunner.query(`ALTER TABLE "users_followers_users" DROP CONSTRAINT "FK_1433e3275a501bc09f5c33c7ca2"`);
        await queryRunner.query(`ALTER TABLE "users_followers_users" DROP CONSTRAINT "FK_8d63f6043394b4d32343bdea11d"`);
        await queryRunner.query(`ALTER TABLE "users_chats_chat" DROP CONSTRAINT "FK_b0c01a6065a43f9bb1bcf460a97"`);
        await queryRunner.query(`ALTER TABLE "users_chats_chat" DROP CONSTRAINT "FK_5937b033223a9f80a2338efe426"`);
        await queryRunner.query(`ALTER TABLE "comments" DROP CONSTRAINT "FK_e44ddaaa6d058cb4092f83ad61f"`);
        await queryRunner.query(`ALTER TABLE "comments" DROP CONSTRAINT "FK_7e8d7c49f218ebb14314fdb3749"`);
        await queryRunner.query(`ALTER TABLE "users" DROP CONSTRAINT "FK_f7297f2289efb670fb2348bfa81"`);
        await queryRunner.query(`ALTER TABLE "likes" DROP CONSTRAINT "FK_e2fe567ad8d305fefc918d44f50"`);
        await queryRunner.query(`ALTER TABLE "likes" DROP CONSTRAINT "FK_cfd8e81fac09d7339a32e57d904"`);
        await queryRunner.query(`ALTER TABLE "posts" DROP CONSTRAINT "FK_ae05faaa55c866130abef6e1fee"`);
        await queryRunner.query(`ALTER TABLE "profileInfos" DROP CONSTRAINT "FK_e514ff7e2d51bcbfdfddfcd72b7"`);
        await queryRunner.query(`ALTER TABLE "message" DROP CONSTRAINT "FK_619bc7b78eba833d2044153bacc"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_dbcc3f102974a9e7213c35edac"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_cba23e8b1a61ac2f4b84060c57"`);
        await queryRunner.query(`DROP TABLE "chat_users_users"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_4a5ef04f52de98c49916c8798e"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_f257a3163914a2e398d7bfa800"`);
        await queryRunner.query(`DROP TABLE "users_following_users"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_1433e3275a501bc09f5c33c7ca"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_8d63f6043394b4d32343bdea11"`);
        await queryRunner.query(`DROP TABLE "users_followers_users"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_b0c01a6065a43f9bb1bcf460a9"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_5937b033223a9f80a2338efe42"`);
        await queryRunner.query(`DROP TABLE "users_chats_chat"`);
        await queryRunner.query(`DROP TABLE "comments"`);
        await queryRunner.query(`DROP TABLE "chat"`);
        await queryRunner.query(`DROP TABLE "users"`);
        await queryRunner.query(`DROP TABLE "likes"`);
        await queryRunner.query(`DROP TABLE "posts"`);
        await queryRunner.query(`DROP TABLE "profileInfos"`);
        await queryRunner.query(`DROP TABLE "message"`);
    }

}