export interface IAccount {
  accountNickname: string;
  userName: string;
  firstName: string;
  lastName: string;
  phone: string;
  address: string;
  address2: string;
  city: string;
  state: string;
  postalCode: number;
  email: string;
  availableCash: number;
  WatchLists: Array<any>;
  Positions: Array<any>;
}