import { BaseInfo } from "./base-info";

export interface ProcessingOrderVO extends BaseInfo {

  id?: number;
  code?: string;
  customerId: number;
  customerName?: string;
  shippingOrderId?: number;
  shippingOrderCode?: string;
  salesman?: string;
  status: number;

  products?: ProcessingOrderProductVO[];

}

export interface ProcessingOrderProductVO {

  id?: number;
  productId: number;
  productName: string;
  type: number;
  density: number;
  productSpecification?: string;
  specification: string;
  quantity: number;
  expectedWeight: number;

}
