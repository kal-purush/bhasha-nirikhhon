import { MigrationInterface, QueryRunner, TableColumn } from "typeorm";

export class AlterTableBooking1703242331393 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.addColumn('booking', new TableColumn({
            name: 'vehicleId',
            type: 'int',
            isNullable: true, 
        }));
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
       
        await queryRunner.dropColumn('booking', 'vehicleId');
    }
}