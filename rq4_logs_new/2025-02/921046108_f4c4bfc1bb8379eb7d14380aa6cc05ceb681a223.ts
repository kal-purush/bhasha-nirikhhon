export interface LocationInfo {
  name: string;
  address: string;
  lat: number;
  lng: number;
  rating?: number;
  userRatingsTotal?: number;
  types?: string[];
  photoUrl?: string;
  openingHours?: google.maps.places.PlaceOpeningHours | {
    weekday_text?: string[];
    isOpen?: () => boolean;
  };
  phoneNumber?: string;
  region?: string;
  category?: string;
}

export interface RestaurantData {
  name: string;
  address: string;
  lat: number;
  lng: number;
  placeId?: string;
  phoneNumber?: string;
  category?: string;
  openingHours?: string;
  description?: string;
  rating?: number;
  ratingCount?: number;
  averagePrice?: number;
  paymentMethods?: string[];
  region?: string;
} 