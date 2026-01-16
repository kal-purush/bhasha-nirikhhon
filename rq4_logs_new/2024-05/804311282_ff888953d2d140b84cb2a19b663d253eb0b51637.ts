import { MigrationInterface, QueryRunner, Table } from 'typeorm';

export class InitialMigration1716400739436 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    // await queryRunner.query(
    //   `CREATE TABLE "users" (
    //     "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
    //     "username" varchar,
    //     "email" varchar(100) NOT NULL,
    //     "password" varchar(50) NOT NULL,
    //     "role" varchar NOT NULL DEFAULT 'user',
    //     "created_at" timestamp NOT NULL DEFAULT now(),
    //     "updated_at" timestamp DEFAULT now(),
    //     CONSTRAINT "UQ_email" UNIQUE ("email"),
    //     CONSTRAINT "PK_id" PRIMARY KEY ("id")
    //   )`,
    // );
    await queryRunner.createTable(
      new Table({
        name: 'users',
        columns: [
          {
            name: 'id',
            type: 'uuid',
            isPrimary: true,
            default: 'uuid_generate_v4()',
          },
          {
            name: 'username',
            type: 'varchar',
            isNullable: true,
          },
          {
            name: 'email',
            type: 'varchar',
            length: '100',
            isUnique: true,
          },
          {
            name: 'password',
            type: 'varchar',
            length: '50',
          },
          {
            name: 'role',
            type: 'varchar',
            default: "'user'",
          },
          {
            name: 'created_at',
            type: 'timestamp',
            default: 'now()',
          },
          {
            name: 'updated_at',
            type: 'timestamp',
            default: 'now()',
            onUpdate: 'now()',
          },
        ],
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // await queryRunner.query(`DROP TABLE "users"`);
    await queryRunner.dropTable('users');
  }
}