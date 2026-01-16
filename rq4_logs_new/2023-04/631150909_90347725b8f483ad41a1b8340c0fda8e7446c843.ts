import type { BrokerConfigType } from "../config";
import type {
  Quote
} from "../types";

import ccxt from "ccxt";
import "dotenv/config";

import { getConfig } from "../config/configLoader";
import { getLogger } from "../logger";
import { toQuote } from "../util";

const { demoMode, symbol } = getConfig();
let keys: object;

if(process.env.BITBANK_TOKEN && process.env.BITBANK_SECRET){
  keys = {
    apiKey: process.env.BITBANK_TOKEN,
    secret: process.env.BITBANK_SECRET,
  };
}else{
  keys = {};
}

async function main(envs: object) {
  // eslint-disable-next-line new-cap
  const base = new ccxt.bitbank(envs);
  return base;
}

export function create(config: BrokerConfigType){
  return new BrokerAdapterImpl(config);
}

export default class BrokerAdapterImpl /*implements BrokerAdapter*/ {
  readonly broker = "Bitbank";
  private readonly logger = getLogger(`${this.broker}Adapter`);
  private readonly api = main(keys);

  constructor(private readonly config: BrokerConfigType) {
    if(Object.keys(keys).length && !demoMode && config.enabled){
      this.logger.fatal(`${this.broker} is enabled but it doesn't have TOKEN and/or SECRET!`);
      process.exit(1);
    }
  }

  async fetchQuotes(): Promise<Quote[]> {
    const response = await (await this.api).fetchOrderBook(symbol);
    return this.mapToQuote(response);
  }

  private mapToQuote(orderBooksResponse: any): Quote[] {
    const asks = orderBooksResponse.asks;
    const mappedAsks = asks.map((q: any) => toQuote(this.broker, "Ask", q[0], q[1]));
    const bids = orderBooksResponse.bids;
    const mappedBids = bids.map((q: any) => toQuote(this.broker, "Bid", q[0], q[1]));
    return mappedAsks.concat(mappedBids);
  }

  async getBtcPosition(): Promise<number> {
    const balance = (await (await this.api).fetchBalance()).info.btc;
    return balance;
  }

  /*
  async send(order: Order): Promise<void> {
    if(order.broker !== this.broker){
      throw new Error();
    }
    const strategy = this.strategyMap.get(order.cashMarginType);
    if(strategy === undefined){
      throw new Error(`Unable to find a strategy for ${order.cashMarginType}.`);
    }
    await strategy.send(order);
  }

  async cancel(order: Order): Promise<void> {
    const orderId = order.brokerOrderId;
    const reply = await this.api.cancelOrder(orderId);
    if(!reply.success){
      throw new Error(`Cancel ${orderId} failed.`);
    }
    order.lastUpdated = new Date();
    order.status = "Canceled";
  }

  async refresh(order: Order): Promise<void> {
    const reply = await this.api.getOpenOrders();
    const brokerOrder = _.find(reply.orders, o => o.id === order.brokerOrderId);
    if(brokerOrder !== undefined){
      if(brokerOrder.pending_amount === undefined || brokerOrder.pending_amount === 0){
        throw new Error("Unexpected reply returned.");
      }
      order.filledSize = eRound(order.size - brokerOrder.pending_amount);
      if(order.filledSize > 0){
        order.status = "PartiallyFilled";
      }
      order.lastUpdated = new Date();
      return;
    }
    const from = addMinutes(order.creationTime, -1);
    const transactions = (await this.api.getTransactionsWithStartDate(from)).filter(
      x => x.order_id === order.brokerOrderId
    );
    if(transactions.length === 0){
      logger.warn("The order is not found in pending orders and historical orders.");
      return;
    }
    order.executions = transactions.map(x => {
      const execution = toExecution(order);
      execution.execTime = x.created_at;
      execution.price = x.rate;
      execution.size = Math.abs(x.funds.btc);
      return execution as Execution;
    });
    order.filledSize = eRound(_.sumBy(order.executions, x => x.size));
    order.status = almostEqual(order.filledSize, order.size, 1) ? "Filled" : "Canceled";
    order.lastUpdated = new Date();
  }
*/
}