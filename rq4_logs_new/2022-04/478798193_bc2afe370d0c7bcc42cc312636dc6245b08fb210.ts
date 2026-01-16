export interface MenuResponse {
  id: string;
  name: string;
  description: string;
  phone: string;
  location: Location;
  operationDays: OperationDayData[];
  privateParking: boolean;
  currency: string;
  menus: MenuData[];
}

export interface LocationData {
  lat: string;
  log: string;
  address1: string;
  address2: string;
  city: string;
  state: string;
  postalCode: string;
  country: string;
}

export interface OperationDayData {
  day: string;
  startAt: string;
  endAt: string;
}

export interface MenuData {
  name: string;
  items: ItemData[];
}

export interface ItemData {
  name: string;
  description: string;
  price: number;
  url: string;
}