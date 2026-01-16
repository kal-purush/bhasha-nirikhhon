interface Address {
  zipCode?: string;
  street?: string;
  number?: string;
  details?: string;
  neighborhood?: string;
  city?: string;
  state?: string;
}

export interface AddressInputModel extends Address {}

export interface AddressOutputModel extends Address {}