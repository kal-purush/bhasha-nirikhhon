import { LatLngLiteral } from '@google/maps';

export default interface UserData {
  phoneNumber?: string;
  homeLocation?: LatLngLiteral & { text: string };
}