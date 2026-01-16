import { OrdineProdotto } from './ordine-prodotto';

export interface Ordine {
  amazonOrderId: string;
  purchaseDate: Date;
  lastUpdateDate: Date;
  orderStatus: string;
  fulfillmentChannel: string;
  numberOfItemsShipped: number;
  numberOfItemsUnshipped: number;
  paymentMethod: string;
  paymentMethodDetails: string;
  marketplaceId: string;
  shipmentServiceLevelCategory: string;
  orderType: string;
  earliestShipDate: Date;
  latestShipDate: Date;
  isBusinessOrder: boolean;
  isPrime: boolean;
  isGlobalExpressEnabled: boolean;
  isPremiumOrder: boolean;
  isSoldByAb: boolean;
  companyLegalName: string;
  buyerEmail: string;
  buyerName: string;
  purchaseOrderNumber: string;
  shippingAddressName: string;
  shippingAddressLine1: string;
  shippingAddressCity: string;
  shippingCityStateOrRegion: string;
  shippingStateOrRegionPostalCode: string;
  ordersItems: OrdineProdotto[];
}