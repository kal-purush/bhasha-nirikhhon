import { MigrationInterface, QueryRunner } from "typeorm";

export class InitDB1671410117336 implements MigrationInterface {
    name = 'InitDB1671410117336'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE \`priority\` (\`id\` varchar(36) NOT NULL, \`name\` varchar(255) NOT NULL, \`desc\` varchar(255) NOT NULL, PRIMARY KEY (\`id\`)) ENGINE=InnoDB`);
        await queryRunner.query(`CREATE TABLE \`task\` (\`id\` varchar(36) NOT NULL, \`name\` varchar(255) NOT NULL, \`desc\` varchar(255) NOT NULL, \`begin_date\` datetime NOT NULL, \`end_date\` datetime NOT NULL, \`task_father\` varchar(255) NULL, \`priority_id\` varchar(36) NULL, \`status_id\` varchar(36) NULL, PRIMARY KEY (\`id\`)) ENGINE=InnoDB`);
        await queryRunner.query(`CREATE TABLE \`status\` (\`id\` varchar(36) NOT NULL, \`name\` varchar(255) NOT NULL, \`desc\` varchar(255) NOT NULL, PRIMARY KEY (\`id\`)) ENGINE=InnoDB`);
        await queryRunner.query(`ALTER TABLE \`task\` ADD CONSTRAINT \`FK_42fc82c4e184b727a3ccd7863ee\` FOREIGN KEY (\`priority_id\`) REFERENCES \`priority\`(\`id\`) ON DELETE NO ACTION ON UPDATE NO ACTION`);
        await queryRunner.query(`ALTER TABLE \`task\` ADD CONSTRAINT \`FK_b8747cc6a41b6cef4639babf61d\` FOREIGN KEY (\`status_id\`) REFERENCES \`status\`(\`id\`) ON DELETE NO ACTION ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE \`task\` DROP FOREIGN KEY \`FK_b8747cc6a41b6cef4639babf61d\``);
        await queryRunner.query(`ALTER TABLE \`task\` DROP FOREIGN KEY \`FK_42fc82c4e184b727a3ccd7863ee\``);
        await queryRunner.query(`DROP TABLE \`status\``);
        await queryRunner.query(`DROP TABLE \`task\``);
        await queryRunner.query(`DROP TABLE \`priority\``);
    }

}