import fp from 'fastify-plugin';
import { FastifyInstance } from 'fastify';
import { IncomingMessage, ServerResponse, Server } from 'http';

declare module 'fastify' {
  export interface FastifyInstance<
    HttpServer = Server,
    HttpRequest = IncomingMessage,
    HttpResponse = ServerResponse
  > {
    authenticate: any;
    updateTransactions: () => void;
  }
}

import cron from 'node-cron';
import fetch from 'node-fetch';

import { getPendingTxs, updateTransactionStatus } from '../db';
import { TransactionStatus, TxStatusPayload } from '../types';

const EXPLORER_API_BASE = 'https://api.blockcypher.com/v1';

export default fp(function transactionsMonitorCron(
  fastify: FastifyInstance<Server, IncomingMessage, ServerResponse>,
  opts,
  next
) {
  const monitor = async () => {
    const pendingTransactions = await getPendingTxs();
    if (!pendingTransactions || pendingTransactions.length === 0) return;
    const txPromises = pendingTransactions.map(async ({ tx_hash: txHash }) => {
      const json: TxStatusPayload = await getTxStatus(txHash);
      if (json) {
        if (json.confirmed) {
          await updateTransactionStatus(txHash, TransactionStatus.passed);
        } else if (json.execution_error) {
          await updateTransactionStatus(txHash, TransactionStatus.failed);
        }
      }
      return json;
    });
    const results = Promise.all(txPromises);
    return results;
  };
  fastify.decorate('updateTransactions', async () => {
    cron.schedule('*/12 * * * * *', monitor);
  });

  fastify.updateTransactions();

  next();
});

async function getTxStatus(hash: string): Promise<TxStatusPayload> {
  const url = `${EXPLORER_API_BASE}/eth/main/txs/${encodeURIComponent(hash)}`;
  const res = await fetch(url);
  if (res.ok) {
    return await res.json();
  } else {
    throw new Error(`Error with request statusCode: ${res.status}`);
  }
}