import { ReactionType } from '@src/entities/ReactionType';
import {
  generateCreatedAtColumn,
  generatePrimaryColumn,
  generateUpdatedAtColumn,
} from 'migrations/__utils/util';
import { MigrationInterface, QueryRunner, Table } from 'typeorm';

export class ReactionType1706439999274 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'reaction_type',
        columns: [
          generatePrimaryColumn('반응 타입 고유 ID'),
          {
            name: 'name',
            type: 'varchar',
            length: '30',
            isNullable: false,
            comment: '반응 타입',
          },
          {
            name: 'memo',
            type: 'varchar',
            length: '255',
            isNullable: false,
            comment: '메모',
          },
          generateCreatedAtColumn(),
          generateUpdatedAtColumn(),
        ],
      }),
    );
    await queryRunner.query('ALTER TABLE reaction_type COMMENT = "반응 타입"');

    await queryRunner.manager.getRepository(ReactionType).upsert(
      [
        {
          name: 'like',
          memo: '좋아요',
        },
      ],
      ['name'],
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropTable('reaction_type');
  }
}