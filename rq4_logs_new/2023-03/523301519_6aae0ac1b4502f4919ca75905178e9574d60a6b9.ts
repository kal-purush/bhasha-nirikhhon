import IUser from "./IUser.model";
import ICountry from "./ICountry.model";

export default interface IAddress {
  addressId: number;
  userId: number;
  streetAndNumber: string;
  zipCode: number;
  city: string;
  countryId: number;
  phoneNumber: string;
  isActive: boolean;
  user?: IUser;
  country?: ICountry;
}

export function formatAddress(address: IAddress): string {
  return (
    address.streetAndNumber +
    ", " +
    address.zipCode +
    " " +
    address.city +
    ", " +
    address.country?.name +
    " (" +
    address.phoneNumber +
    ")"
  );
}