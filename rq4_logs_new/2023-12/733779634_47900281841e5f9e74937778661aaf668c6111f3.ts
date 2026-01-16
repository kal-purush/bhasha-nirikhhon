import {MigrationInterface, QueryRunner, Table} from 'typeorm';

export class CreateUserTable1612345678901 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.createTable(new Table({
            name: 'user',
            columns: [
                { name: 'id', type: 'serial', isPrimary: true },
                { name: 'username', type: 'varchar', isNullable: false },
                { name: 'email', type: 'varchar', isNullable: false },
            ],
        }), true);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.dropTable('user');
    }
}