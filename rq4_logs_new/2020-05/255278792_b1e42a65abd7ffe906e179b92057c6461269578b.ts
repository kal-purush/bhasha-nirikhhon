import {MigrationInterface, QueryRunner} from "typeorm";

export class UpdateBookFieldIsRental1589537895718 implements MigrationInterface {
    name = 'UpdateBookFieldIsRental1589537895718'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query("ALTER TABLE `rental` DROP FOREIGN KEY `FK_79c5e75e6d3b8cab309347ff4c3`", undefined);
        await queryRunner.query("DROP INDEX `REL_79c5e75e6d3b8cab309347ff4c` ON `rental`", undefined);
        await queryRunner.query("ALTER TABLE `rental` CHANGE `returnDateAt` `returnDateAt` timestamp NULL DEFAULT NULL", undefined);
        await queryRunner.query("ALTER TABLE `rental` ADD CONSTRAINT `FK_79c5e75e6d3b8cab309347ff4c3` FOREIGN KEY (`bookId`) REFERENCES `book`(`id`) ON DELETE CASCADE ON UPDATE NO ACTION", undefined);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query("ALTER TABLE `rental` DROP FOREIGN KEY `FK_79c5e75e6d3b8cab309347ff4c3`", undefined);
        await queryRunner.query("ALTER TABLE `rental` CHANGE `returnDateAt` `returnDateAt` timestamp NULL", undefined);
        await queryRunner.query("CREATE UNIQUE INDEX `REL_79c5e75e6d3b8cab309347ff4c` ON `rental` (`bookId`)", undefined);
        await queryRunner.query("ALTER TABLE `rental` ADD CONSTRAINT `FK_79c5e75e6d3b8cab309347ff4c3` FOREIGN KEY (`bookId`) REFERENCES `book`(`id`) ON DELETE NO ACTION ON UPDATE NO ACTION", undefined);
    }

}