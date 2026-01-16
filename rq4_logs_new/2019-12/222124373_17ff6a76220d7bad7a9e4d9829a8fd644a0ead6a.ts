import { MigrationInterface, QueryRunner } from 'typeorm';

export class PostRefactoring1577018262936 implements MigrationInterface {
    name = 'PostRefactoring1577018262936';

    public async up (queryRunner: QueryRunner): Promise<any> {
        await queryRunner.query('ALTER TABLE `recipe` DROP FOREIGN KEY `FK_269b0269b7957322c77d30e4795`', undefined);
        await queryRunner.query('DROP INDEX `REL_269b0269b7957322c77d30e479` ON `recipe`', undefined);
        await queryRunner.query('ALTER TABLE `recipe` DROP COLUMN `analyzedInstructionsId`', undefined);
        await queryRunner.query('ALTER TABLE `instruction` ADD `recipeId` int NULL', undefined);
        await queryRunner.query('ALTER TABLE `instruction` ADD CONSTRAINT `FK_8ac131565357b5fb601d5728ab0` FOREIGN KEY (`recipeId`) REFERENCES `recipe`(`id`) ON DELETE NO ACTION ON UPDATE NO ACTION', undefined);
    }

    public async down (queryRunner: QueryRunner): Promise<any> {
        await queryRunner.query('ALTER TABLE `instruction` DROP FOREIGN KEY `FK_8ac131565357b5fb601d5728ab0`', undefined);
        await queryRunner.query('ALTER TABLE `instruction` DROP COLUMN `recipeId`', undefined);
        await queryRunner.query('ALTER TABLE `recipe` ADD `analyzedInstructionsId` int NULL', undefined);
        await queryRunner.query('CREATE UNIQUE INDEX `REL_269b0269b7957322c77d30e479` ON `recipe` (`analyzedInstructionsId`)', undefined);
        await queryRunner.query('ALTER TABLE `recipe` ADD CONSTRAINT `FK_269b0269b7957322c77d30e4795` FOREIGN KEY (`analyzedInstructionsId`) REFERENCES `instruction`(`id`) ON DELETE NO ACTION ON UPDATE NO ACTION', undefined);
    }
}