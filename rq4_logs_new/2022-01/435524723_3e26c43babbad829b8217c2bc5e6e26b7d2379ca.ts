import { SortType } from "./shared";

export enum TradeStatus {
  TradeOpen = 'Open',
  TradeAccepted = 'Accepted',
  TradeRejected = 'Rejected',
}

export interface TradeResourceWrite {
  resource_id: number;
  quantity: number;
}

export interface TradeInventoryWrite {
  survivor_id: number;
  items: TradeResourceWrite[];
}

export interface TradeWrite {
  sender: TradeInventoryWrite;
  receiver: TradeInventoryWrite;
}

export interface TradeRejectWrite {
  trade_id: number;
  annotation?: string;
}

export interface TradeFilter {
  name: string;
  sort: SortType;
  page: number;
  page_items?: number;
}

export interface TradeHistorySurvivorRead {
  id: number;
  name: string;
}

export interface TradeHistoryItemRead {
  item_name: string;
  item_quantity: number;
}

export interface TradeRead {
  id: number;
  status: TradeStatus;
  sender: TradeHistorySurvivorRead;
  receiver: TradeHistorySurvivorRead;
  sender_items: TradeHistoryItemRead[];
  receiver_items: TradeHistoryItemRead[];
}