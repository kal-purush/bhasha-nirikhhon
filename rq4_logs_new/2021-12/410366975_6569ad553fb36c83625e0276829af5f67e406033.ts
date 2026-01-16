export interface PaymentData {
    email: string | null;
    amount: string | null | undefined;
    currency: string | null;
    mobile_money?: MobileMoney;
    callback_url?: string;
    price: string | null | undefined;
  };

export interface MobileMoney {
  phone: string | undefined;
  provider: string | undefined;
}

export interface BusinessTransactionData {
  user_id: string | undefined;
  initiator_id: string | undefined;
  initiator_role: string | undefined;
  name: string | undefined;
  items: BusinessTransactionItem[] | undefined;
  price: any;
  description: string | undefined;
  delivery_phone: string | undefined;
  currency: string | undefined;
  payment_id: string | undefined;
  callback_url: string | undefined;
  cancel_url?: string;
  order_id?:string;
}

export interface BusinessTransactionItem {
name: string | undefined;
item_id: string | undefined;
items_qty: string | undefined;
price: string | undefined;
description: string | undefined;
}