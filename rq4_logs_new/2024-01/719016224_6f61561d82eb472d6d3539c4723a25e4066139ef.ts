import {
  generateCreatedAtColumn,
  generateDeletedAtColumn,
  generatePrimaryColumn,
  generateUpdatedAtColumn,
} from 'migrations/__utils/util';
import { MigrationInterface, QueryRunner, Table } from 'typeorm';

export class FreePost1706440108331 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    // 자유 게시글
    await queryRunner.createTable(
      new Table({
        name: 'free_post',
        columns: [
          generatePrimaryColumn('자유 게시글 고유 ID'),
          {
            name: 'user_id',
            type: 'int',
            unsigned: true,
            isNullable: false,
            comment: '게시글 작성 유저 고유 ID',
          },
          {
            name: 'title',
            type: 'varchar',
            length: '255',
            isNullable: false,
            comment: '자유게시글 제목',
          },
          {
            name: 'description',
            type: 'text',
            isNullable: false,
            comment: '자유게시글 내용',
          },
          {
            name: 'hit',
            type: 'int',
            unsigned: true,
            isNullable: false,
            default: 0,
            comment: '조회수',
          },
          {
            name: 'is_anonymous',
            type: 'tinyint',
            length: '1',
            unsigned: true,
            default: 0,
            isNullable: false,
            comment: '작성자 익명 여부 (0: 실명, 1: 익명)',
          },
          {
            name: 'status',
            type: 'enum',
            enum: ['posting', 'remove'],
            isNullable: false,
            default: `"posting"`,
            comment: '자유게시글 상태',
          },
          generateCreatedAtColumn(),
          generateUpdatedAtColumn(),
          generateDeletedAtColumn(),
        ],
        foreignKeys: [
          {
            referencedTableName: 'user',
            referencedColumnNames: ['id'],
            columnNames: ['user_id'],
            onDelete: 'CASCADE',
            onUpdate: 'CASCADE',
          },
        ],
      }),
    );
    await queryRunner.query('ALTER TABLE free_post COMMENT = "자유 게시판"');
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropTable('free_post');
  }
}