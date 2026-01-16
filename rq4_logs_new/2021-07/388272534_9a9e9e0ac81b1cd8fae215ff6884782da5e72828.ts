export interface SearchInput {
  lat: number;
  lng: number;
}

interface Amenity {
  code: number;
  formatted: string;
}

interface Image {
  altText: string;
  height: number;
  isHeroImage: Boolean;
  url: string;
  width: number;
}

export interface HotelData {
  address: {
    line1: string;
    line2: string;
    city: string;
    postalCode: string;
    region: string;
    country: string;
    countryName: string;
  };
  amenities: Array<Amenity>;
  checkIn: {
    from: string;
  };
  checkOut: {
    to: string;
  };
  createdAt: string;
  currency: string;
  description: {
    short: string;
  };
  emails: Array<string>;
  externalUrls: Array<string>;
  hotelId: string;
  images: Array<Image>;
  location: {
    longitude: number;
    latitude: number;
  };
  name: string;
  phonenumbers: Array<string>;
  roomCount: number;
  roomTypes: Array<{
    amenities: Amenity;
    description: string;
    images: Image;
    starRating: number;
    termsAndConditions: string;
    updatedAt: string;
    websiteUrl: string;
  }>;
  starRating: number;
  termsAndConditions: string;
  updatedAt: string;
  websiteUrl: string;
}