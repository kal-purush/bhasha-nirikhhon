import { MigrationInterface, QueryRunner, TableColumn } from "typeorm"

export class AlterUser1703826879213 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.addColumn(
            'user',
            new TableColumn({
              name: 'password',
              type: 'varchar',
              isNullable: true,
            }),
        );

        await queryRunner.addColumn(
            'user',
            new TableColumn({
              name: 'auth',
              type: 'varchar',
              isNullable: true,
            }),
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.dropColumn('user', 'password');
        await queryRunner.dropColumn('user', 'auth');
    }

}