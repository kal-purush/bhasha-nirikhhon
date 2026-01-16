import { FastifyInstance } from 'fastify';

export class AbstractRepository {
  query!: FastifyInstance['database']['query'];
  firstRow!: FastifyInstance['database']['firstRow'];
  allRows!: FastifyInstance['database']['allRows'];
  transaction!: FastifyInstance['database']['transaction'];

  register({ database: { query, firstRow, allRows, transaction } }: FastifyInstance) {
    this.query = query;
    this.firstRow = firstRow;
    this.allRows = allRows;
    this.transaction = transaction;
  }
}