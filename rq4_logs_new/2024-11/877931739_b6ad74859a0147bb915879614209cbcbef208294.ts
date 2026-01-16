import { MigrationInterface, QueryRunner } from "typeorm";

export class AddAllTables1731685135801 implements MigrationInterface {
    name = 'AddAllTables1731685135801'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "Users" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "login" character varying(255) COLLATE "C" NOT NULL, "password" character varying NOT NULL, "email" character varying NOT NULL, "createdAt" TIMESTAMP NOT NULL DEFAULT now(), "confirmationCode" character varying NOT NULL DEFAULT '', "expirationDate" character varying NOT NULL DEFAULT '', "isConfirmed" boolean NOT NULL DEFAULT false, CONSTRAINT "PK_16d4f7d636df336db11d87413e3" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "Sessions" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "user_id" uuid NOT NULL, "device_id" uuid NOT NULL, "iat" character varying NOT NULL, "exp" character varying NOT NULL, "device_name" character varying NOT NULL, "ip" character varying NOT NULL, CONSTRAINT "PK_0ff5532d98863bc618809d2d401" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "Blogs" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "name" character varying COLLATE "C" NOT NULL, "description" text NOT NULL, "websiteUrl" character varying NOT NULL, "createdAt" TIMESTAMP NOT NULL DEFAULT now(), "isMembership" boolean NOT NULL DEFAULT false, CONSTRAINT "PK_007e2aca1eccf50f10c9176a71c" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "Posts" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "title" character varying COLLATE "C" NOT NULL, "shortDescription" text NOT NULL, "content" text NOT NULL, "blogId" uuid NOT NULL, "createdAt" TIMESTAMP NOT NULL DEFAULT now(), CONSTRAINT "PK_0f050d6d1112b2d07545b43f945" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "Comments" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "content" text NOT NULL, "createdAt" TIMESTAMP NOT NULL DEFAULT now(), "postId" uuid NOT NULL, "userId" uuid NOT NULL, CONSTRAINT "PK_91e576c94d7d4f888c471fb43de" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "CommentsLikes" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "likeStatus" character varying NOT NULL, "userId" uuid NOT NULL, "commentsId" uuid NOT NULL, "createdAt" TIMESTAMP NOT NULL DEFAULT now(), CONSTRAINT "PK_6211beadd299640d355c8c38b1c" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TABLE "PostsLikes" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "likeStatus" character varying NOT NULL, "userId" uuid NOT NULL, "postId" uuid NOT NULL, "createdAt" TIMESTAMP NOT NULL DEFAULT now(), CONSTRAINT "PK_35ec440113df798a372e419ad30" PRIMARY KEY ("id"))`);
        await queryRunner.query(`ALTER TABLE "Sessions" ADD CONSTRAINT "FK_28c544a997944cf3bb6f09e9d5e" FOREIGN KEY ("user_id") REFERENCES "Users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "Posts" ADD CONSTRAINT "FK_3d48d13b4578bccfbda468b1c4c" FOREIGN KEY ("blogId") REFERENCES "Blogs"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "Comments" ADD CONSTRAINT "FK_68844d71da70caf0f0f4b0ed72d" FOREIGN KEY ("postId") REFERENCES "Posts"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "Comments" ADD CONSTRAINT "FK_aa80cd9ae4c341f0aeba2401b10" FOREIGN KEY ("userId") REFERENCES "Users"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "CommentsLikes" ADD CONSTRAINT "FK_41dc91cf58a79ec5f19c5c15e32" FOREIGN KEY ("userId") REFERENCES "Users"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "CommentsLikes" ADD CONSTRAINT "FK_31762978eb2fe52abd4c2be9b06" FOREIGN KEY ("commentsId") REFERENCES "Comments"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "PostsLikes" ADD CONSTRAINT "FK_e317dfc5787bc03ef88595afec1" FOREIGN KEY ("userId") REFERENCES "Users"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE "PostsLikes" ADD CONSTRAINT "FK_2d67bf08aef0cafa65a9cbfce5d" FOREIGN KEY ("postId") REFERENCES "Posts"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "PostsLikes" DROP CONSTRAINT "FK_2d67bf08aef0cafa65a9cbfce5d"`);
        await queryRunner.query(`ALTER TABLE "PostsLikes" DROP CONSTRAINT "FK_e317dfc5787bc03ef88595afec1"`);
        await queryRunner.query(`ALTER TABLE "CommentsLikes" DROP CONSTRAINT "FK_31762978eb2fe52abd4c2be9b06"`);
        await queryRunner.query(`ALTER TABLE "CommentsLikes" DROP CONSTRAINT "FK_41dc91cf58a79ec5f19c5c15e32"`);
        await queryRunner.query(`ALTER TABLE "Comments" DROP CONSTRAINT "FK_aa80cd9ae4c341f0aeba2401b10"`);
        await queryRunner.query(`ALTER TABLE "Comments" DROP CONSTRAINT "FK_68844d71da70caf0f0f4b0ed72d"`);
        await queryRunner.query(`ALTER TABLE "Posts" DROP CONSTRAINT "FK_3d48d13b4578bccfbda468b1c4c"`);
        await queryRunner.query(`ALTER TABLE "Sessions" DROP CONSTRAINT "FK_28c544a997944cf3bb6f09e9d5e"`);
        await queryRunner.query(`DROP TABLE "PostsLikes"`);
        await queryRunner.query(`DROP TABLE "CommentsLikes"`);
        await queryRunner.query(`DROP TABLE "Comments"`);
        await queryRunner.query(`DROP TABLE "Posts"`);
        await queryRunner.query(`DROP TABLE "Blogs"`);
        await queryRunner.query(`DROP TABLE "Sessions"`);
        await queryRunner.query(`DROP TABLE "Users"`);
    }

}