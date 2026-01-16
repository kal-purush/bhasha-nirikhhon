import { MigrationInterface, QueryRunner } from "typeorm";

export class PagamentoIndevido1727912304459 implements MigrationInterface {
    name = 'PagamentoIndevido1727912304459'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "pagamento_indevido" ("id" integer NOT NULL, "dataPagamento" DATE NOT NULL, "dataReferencia" DATE NULL, "nomeFavorecido" character varying(150) NOT NULL, "valorPago" NUMERIC NOT NULL, "valorDebitar" NUMERIC NOT NULL, CONSTRAINT "PK_PagamentoIndevido_id" PRIMARY KEY ("id"))`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "pagamento_indevido"`);
    }

}