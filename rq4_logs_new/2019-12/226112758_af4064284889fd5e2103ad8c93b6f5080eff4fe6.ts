import { BaseInfo } from "./base-info";

export interface ArrearOrderInfoVO extends BaseInfo {

  id?: number;
  code?: string;
  shippingOrderId?: number;
  shippingOrderCode?: string;
  status?: number;
  invoiceNumber?: string;
  customerId?: number;
  customerName?: string;
  receivableCash?: number;
  receivedCash?: number;
  dueDate?: string;
  overDue?: boolean;

  receipts?: ArrearOrderReceiptRecordVO[];
}

export interface ArrearOrderReceiptRecordVO extends BaseInfo {

  id?: number;
  status?: number;
  cash?: number;
  salesman?: string;
  description?: string;
  doneAt?: string;

}