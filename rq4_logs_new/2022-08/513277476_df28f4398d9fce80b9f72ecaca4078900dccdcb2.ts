import { MigrationInterface, QueryRunner, Table } from "typeorm"

export class CriarConteudo1660687660691 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.createTable(
            new Table({
                name: 'conteudos',
                columns: [
                    {
                        name: 'id',
                        type: 'uuid',
                        isPrimary: true,
                    },
                    {
                        name: "titulo",
                        type: "varchar",
                    },
                    {
                        name: "resumo",
                        type: "varchar",
                    },
                    {
                        name: "elaboracao",
                        type: "varchar",
                    },
                    {
                        name: "assunto_id",
                        type: "uuid",
                        isNullable: true,
                    },
                    {
                        name: "criado_em",
                        type: "timestamp",
                        default: "now()",
                    },
                    {
                        name: "atualizado_em",
                        type: "timestamp",
                        default: "now()",
                    },
                ],
                foreignKeys: [
                    {
                        name: "FKAssuntosConteudos",
                        referencedTableName: "assuntos",
                        referencedColumnNames: ["id"],
                        columnNames: ["assunto_id"],
                        onDelete: "SET NULL",
                        onUpdate: "SET NULL",
                    }
                ],
            })
        )
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.dropTable('conteudos')
    }

}