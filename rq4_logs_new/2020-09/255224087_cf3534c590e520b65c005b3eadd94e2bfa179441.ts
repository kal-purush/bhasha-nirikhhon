import {MigrationInterface, QueryRunner} from "typeorm";

export class webhookInstanceSetting1599309954647 implements MigrationInterface {
    name = 'webhookInstanceSetting1599309954647'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "meta" ADD "enableWebhookNotification" boolean NOT NULL DEFAULT false`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "meta" DROP COLUMN "enableWebhookNotification"`);
    }

}