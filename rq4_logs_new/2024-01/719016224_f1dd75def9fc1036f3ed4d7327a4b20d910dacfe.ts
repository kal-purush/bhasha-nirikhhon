import {
  QueryRunner,
  Table,
  TableColumn,
  TableColumnOptions,
  TableForeignKey,
} from 'typeorm';

export const generatePrimaryColumn = (
  comment: string = '고유 ID',
): TableColumnOptions => {
  return {
    name: 'id',
    type: 'int',
    unsigned: true,
    isPrimary: true,
    isNullable: false,
    isGenerated: true,
    generationStrategy: 'increment',
    comment,
  };
};

export const generateCreatedAtColumn = (
  comment: string = '생성 일자',
): TableColumnOptions => {
  return {
    name: 'created_at',
    type: 'timestamp',
    isNullable: false,
    default: 'CURRENT_TIMESTAMP',
    comment,
  };
};

export const generateUpdatedAtColumn = (
  comment: string = '수정 일자',
): TableColumnOptions => {
  return {
    name: 'updated_at',
    type: 'timestamp',
    isNullable: false,
    default: 'CURRENT_TIMESTAMP',
    onUpdate: 'CURRENT_TIMESTAMP',
    comment,
  };
};

export const generateDeletedAtColumn = (
  comment: string = '삭제 일자',
): TableColumnOptions => {
  return {
    name: 'deleted_at',
    type: 'timestamp',
    isNullable: true,
    comment,
  };
};

export const createHistoryTable = async (
  queryRunner: QueryRunner,
  originTableName: string,
  options: {
    dropColumns?: string[];
    onDelete?: string;
    onUpdate?: string;
  } = {},
) => {
  const {
    dropColumns = ['updated_at', 'deleted_at'],
    onDelete = 'CASCADE',
    onUpdate = 'CASCADE',
  } = options;
  const historyTableName = `${originTableName}_history`;
  const fkColumnName = `${originTableName}_id`;

  await queryRunner.query(
    `CREATE TABLE \`${historyTableName}\` LIKE \`${originTableName}\`;`,
  );
  await queryRunner.dropColumns(historyTableName, dropColumns);
  await queryRunner.addColumn(
    historyTableName,
    new TableColumn({
      name: fkColumnName,
      type: 'int',
      unsigned: true,
      isNullable: false,
      comment: 'origin 고유 ID',
    }),
  );
  await queryRunner.createForeignKey(
    historyTableName,
    new TableForeignKey({
      columnNames: [fkColumnName],
      referencedTableName: originTableName,
      referencedColumnNames: ['id'],
      onDelete,
      onUpdate,
    }),
  );
};

export const createReactionTable = async (
  queryRunner: QueryRunner,
  parentTableName: string,
  parentDescription: string,
) => {
  const reactionTableName = `${parentTableName}_reaction`;
  const fkColumnName = `${parentTableName}_id`;

  await queryRunner.createTable(
    new Table({
      name: reactionTableName,
      columns: [
        generatePrimaryColumn(),
        {
          name: 'reaction_type_id',
          type: 'int',
          unsigned: true,
          isNullable: false,
          comment: '반응 타입 고유 ID',
        },
        {
          name: 'user_id',
          type: 'int',
          unsigned: true,
          isNullable: false,
          comment: '유저 고유 ID',
        },
        {
          name: fkColumnName,
          type: 'int',
          unsigned: true,
          isNullable: false,
          comment: `${parentDescription} 고유 ID`,
        },
        generateCreatedAtColumn(),
      ],
      foreignKeys: [
        {
          referencedTableName: 'reaction_type',
          referencedColumnNames: ['id'],
          columnNames: ['reaction_type_id'],
          onDelete: 'CASCADE',
          onUpdate: 'CASCADE',
        },
        {
          referencedTableName: 'user',
          referencedColumnNames: ['id'],
          columnNames: ['user_id'],
          onDelete: 'CASCADE',
          onUpdate: 'CASCADE',
        },
        {
          referencedTableName: parentTableName,
          referencedColumnNames: ['id'],
          columnNames: [fkColumnName],
          onDelete: 'CASCADE',
          onUpdate: 'CASCADE',
        },
      ],
    }),
  );
  await queryRunner.query(
    `ALTER TABLE \`${reactionTableName}\` COMMENT = "${parentDescription} 게시글 반응"`,
  );
};

export const createCommentTable = async (
  queryRunner: QueryRunner,
  postInfo: {
    name: string;
    description: string;
  },
  addColumns: TableColumn[] = [],
) => {
  const { name: postTableName, description: postDescription } = postInfo;

  const commentTableName = `${postTableName}_comment`;
  const postFkColumnName = `${postTableName}_id`;

  await queryRunner.createTable(
    new Table({
      name: commentTableName,
      columns: [
        generatePrimaryColumn(`${postDescription} 댓글 고유 ID`),
        {
          name: 'user_id',
          type: 'int',
          unsigned: true,
          isNullable: false,
          comment: '댓글 작성 유저 고유 ID',
        },
        {
          name: postFkColumnName,
          type: 'int',
          unsigned: true,
          isNullable: false,
          comment: `${postDescription} 고유 ID`,
        },
        {
          name: 'description',
          type: 'varchar',
          length: '255',
          isNullable: false,
          comment: '댓글 본문',
        },
        {
          name: 'is_anonymous',
          type: 'tinyint',
          length: '1',
          default: 0,
          unsigned: true,
          isNullable: false,
          comment: '작성자 익명 여부 (0: 실명, 1: 익명)',
        },
        ...addColumns,
        {
          name: 'status',
          type: 'enum',
          enum: ['posting', 'remove'],
          default: `"posting"`,
          isNullable: false,
          comment: `${postDescription} 댓글 상태`,
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
        {
          referencedTableName: postTableName,
          referencedColumnNames: ['id'],
          columnNames: [postFkColumnName],
          onDelete: 'CASCADE',
          onUpdate: 'CASCADE',
        },
      ],
    }),
  );
  await queryRunner.query(
    `ALTER TABLE \`${commentTableName}\` COMMENT = "${postDescription} 게시글 댓글"`,
  );
};

export const createReplyCommentTable = async (
  queryRunner: QueryRunner,
  postInfo: {
    name: string;
    description: string;
  },
  addColumns: TableColumn[] = [],
) => {
  const { name: postTableName, description: postDescription } = postInfo;

  const commentTableName = `${postTableName}_comment`;
  const replyCommentTableName = `${postTableName}_reply_comment`;
  const postFkColumnName = `${postTableName}_id`;
  const commentFkColumnName = `${commentTableName}_id`;
  const commentDescription = `${postDescription} 댓글`;

  await queryRunner.createTable(
    new Table({
      name: replyCommentTableName,
      columns: [
        generatePrimaryColumn(`${postDescription} 대댓글 고유 ID`),
        {
          name: 'user_id',
          type: 'int',
          unsigned: true,
          isNullable: false,
          comment: '댓글 작성 유저 고유 ID',
        },
        {
          name: postFkColumnName,
          type: 'int',
          unsigned: true,
          isNullable: false,
          comment: `${postDescription} 고유 ID`,
        },
        {
          name: commentFkColumnName,
          type: 'int',
          unsigned: true,
          isNullable: false,
          comment: `${commentDescription} 고유 ID`,
        },
        {
          name: 'description',
          type: 'varchar',
          length: '255',
          isNullable: false,
          comment: '대댓글 본문',
        },
        {
          name: 'is_anonymous',
          type: 'tinyint',
          length: '1',
          default: 0,
          unsigned: true,
          isNullable: false,
          comment: '작성자 익명 여부 (0: 실명, 1: 익명)',
        },
        ...addColumns,
        {
          name: 'status',
          type: 'enum',
          enum: ['posting', 'remove'],
          default: `"posting"`,
          isNullable: false,
          comment: `${postDescription} 대댓글 상태`,
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
        {
          referencedTableName: postTableName,
          referencedColumnNames: ['id'],
          columnNames: [postFkColumnName],
          onDelete: 'CASCADE',
          onUpdate: 'CASCADE',
        },
        {
          referencedTableName: commentTableName,
          referencedColumnNames: ['id'],
          columnNames: [commentFkColumnName],
          onDelete: 'CASCADE',
          onUpdate: 'CASCADE',
        },
      ],
    }),
  );
  await queryRunner.query(
    `ALTER TABLE \`${replyCommentTableName}\` COMMENT = "${postDescription} 게시글 대댓글"`,
  );
};