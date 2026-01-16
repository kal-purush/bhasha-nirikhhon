import { MigrationInterface, QueryRunner, Table } from 'typeorm';

export class createEvents1602632546990 implements MigrationInterface {

  public async up(queryRunner: QueryRunner): Promise<void> {
    // Build or remove things
    await queryRunner.createTable(new Table({
      name: 'charity_events',
      columns: [
        {
          name: 'id',
          type: 'integer',
          unsigned: true,
          isPrimary: true,
          isGenerated: true,
          generationStrategy: 'increment'
        },
        {
          name: 'name',
          type: 'varchar'
        },
        {
          name: 'latitude',
          type: 'decimal',
          scale: 10,
          precision: 2
        },
        {
          name: 'longitude',
          type: 'decimal',
          scale: 10,
          precision: 2
        },
        {
          name: 'about',
          type: 'text'
        },
        {
          name: 'instructions',
          type: 'text'
        },
        {
          name: 'start_hours',
          type: 'varchar'
        },
        {
          name: 'occurs_on_weekends',
          type: 'boolean',
          default: false
        }
      ]
    }))
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Rollback
    await queryRunner.dropTable('charity_events')
  }

}