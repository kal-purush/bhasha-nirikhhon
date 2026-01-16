
export interface PlanOptionProps {
    name: string;
    price: number;
    value: string;
    savings?: number;
    features?: string[];
  }
  
  export interface BillingAddressProps {
    country: string;
    state: string;
    onCountryChange: (country: string) => void;
    onStateChange: (state: string) => void;
  }
  
  export interface PaymentMethodProps {
    cardNumber: string;
    expirationDate: string;
    securityCode: string;
    cardHolderName: string;
    onCardNumberChange: (value: string) => void;
    onExpirationDateChange: (value: string) => void;
    onSecurityCodeChange: (value: string) => void;
    onCardHolderNameChange: (value: string) => void;
  }
  