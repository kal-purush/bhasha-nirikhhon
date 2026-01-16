import { env } from '@/config/env.config';
import { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateShortenedUrlsTable1736838705897 implements MigrationInterface {
	public async up(queryRunner: QueryRunner): Promise<void> {
		await queryRunner.query(`
            CREATE TABLE "shortened_urls" (
            id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id uuid NOT NULL,
            original_url character varying NOT NULL,
            short_code character varying(8) NOT NULL,
            expires_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW() + INTERVAL '${env.DEFAULT_URL_EXPIRY_TIME}',
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            deleted_at TIMESTAMP WITH TIME ZONE,
            CONSTRAINT FK_shortened_urls_user FOREIGN KEY (user_id)
                REFERENCES users (id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT unique_shortened_urls_short_code UNIQUE (short_code) 
            );
        `);
	}

	public async down(queryRunner: QueryRunner): Promise<void> {
		await queryRunner.query(`
            DROP TABLE shortened_urls
            `);
	}
}