class PriceSizePair {
  price: number;
  size: number;
}

export class BoardResponse {
  mid_price: number;
  bids: PriceSizePair[];
  asks: PriceSizePair[];
}

export interface SendChildOrderRequest {
  product_code: string;
  child_order_type: string;
  side: string;
  price?: number;
  size: number;
  minute_to_expire?: number;
  time_in_force?: string;
}

export class SendChildOrderResponse {
  child_order_acceptance_id: string;
}

export interface CancelChildOrderRequest {
  product_code: string;
  child_order_acceptance_id?: string;
  child_order_id?: string;
}

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface CancelChildOrderResponse {}

export interface ExecutionsParam {
  product_code?: string;
  count?: number;
  before?: number;
  after?: number;
  child_order_id?: string;
  child_order_acceptance_id?: string;
}

export class Execution {
  id: number;
  child_order_id: string;
  side: string;
  price: number;
  size: number;
  commission: number;
  exec_date: Date;
  child_order_acceptance_id: string;
}

export type ExecutionsResponse = Execution[];

export class Balance {
  currency_code: string;
  amount: number;
  available: number;
}

export type BalanceResponse = Balance[];

export interface ChildOrdersParam {
  product_code?: string;
  count?: number;
  before?: number;
  after?: number;
  child_order_state?: string;
  child_order_id?: string;
  child_order_acceptance_id?: string;
  parent_order_id?: string;
}

export class ChildOrder {
  id: number;
  child_order_id: string;
  product_code: string;
  side: string;
  child_order_type: string;
  price: number;
  average_price: number;
  size: number;
  child_order_state: string;
  expire_date: Date;
  child_order_date: Date;
  child_order_acceptance_id: string;
  outstanding_size: number;
  cancel_size: number;
  executed_size: number;
  total_commission: number;
}

export type ChildOrdersResponse = ChildOrder[];