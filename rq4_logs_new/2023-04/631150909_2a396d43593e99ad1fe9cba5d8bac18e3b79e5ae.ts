import type { BrokerConfigType } from "../config";
import type {
  Order,
  Execution,
  BrokerAdapter,
  Quote
} from "../types";

import ccxt from "ccxt";
import { addMinutes } from "date-fns";
import "dotenv/config";

import { getLogger } from "../logger";
import { eRound, almostEqual, toExecution, toQuote } from "../util";

let keys: object;
const logger = getLogger("Coincheck");

if(process.env.COINCHECK_TOKEN && process.env.COINCHECK_SECRET){
  keys = {
    apiKey: process.env.COINCHECK_TOKEN,
    secret: process.env.COINCHECK_SECRET,
  };
}else{
  keys = {};
}

async function main(envs: object) {
  // eslint-disable-next-line new-cap
  const base = new ccxt.bitflyer(envs);
  return base;
}

export function create(config: BrokerConfigType){
  return new BrokerAdapterImpl(config);
}
export default class BrokerAdapterImpl /*implements BrokerAdapter*/ {
  readonly broker = "Bitflyer";
  private readonly api = main(keys);

  constructor(private readonly config: BrokerConfigType) {
  }

  async fetchQuotes(): Promise<Quote[]> {
    const response = await (await this.api).fetchOrderBook("BTC/JPY");
    return this.mapToQuote(response);
  }

  private mapToQuote(orderBooksResponse: any): Quote[] {
    const asks = orderBooksResponse.asks;
    const mappedAsks = new Array(asks.map((q: any) => toQuote(this.broker, "Ask", q[0], q[1])));
    const bids = orderBooksResponse.bids;
    const mappedBids = new Array(bids.map((q: any) => toQuote(this.broker, "Bid", q[0], q[1])));
    return mappedAsks.concat(mappedBids);
  }

  async getBtcPosition(): Promise<number> {
    const balance = (await (await this.api).fetchBalance()).info;
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