import { MigrationInterface, QueryRunner, TableColumn, TableIndex } from 'typeorm';

export class UpdateUsersTable1612345678903 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.addColumns('user', [
            new TableColumn({ name: 'firstName', type: 'varchar', isNullable: true }),
            new TableColumn({ name: 'lastName', type: 'varchar', isNullable: true }),
            new TableColumn({ name: 'createdAt', type: 'timestamp', default: 'CURRENT_TIMESTAMP', isNullable: false }),
            new TableColumn({ name: 'updatedAt', type: 'timestamp', default: 'CURRENT_TIMESTAMP', onUpdate: 'CURRENT_TIMESTAMP', isNullable: false }),
        ]);

        await queryRunner.createIndex('user', new TableIndex({ columnNames: ['username'], isUnique: true }));
        await queryRunner.createIndex('user', new TableIndex({ columnNames: ['email'], isUnique: true }));
        
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.dropColumns('user', ['firstName', 'lastName', 'createdAt', 'updatedAt']);

        await queryRunner.dropIndex('user', 'IDX_UNIQUE_USERNAME');
        await queryRunner.dropIndex('user', 'IDX_UNIQUE_EMAIL');
    }
}