export interface ISneaker {
   productId: string;
   slug: string;
   styleCode: string;
   categoryNames: string[];
   brandNames: string[];
   brandLineNames: string[];
   productName: string;
   description: string;
   variantsCount: number;
   sameDayShipping: boolean;
   sdsPriceWithTaxMin: number;
   sizeType: number[];
   conditions: Condition[];
   customMappings: CustomMappings;
   priceWithTax: PriceWithTax;
   productAsset: ProductAsset;
}

export interface Condition {
   condition: string;
}

export interface CustomMappings {
   featured: boolean;
   new: boolean;
}

export interface PriceWithTax {
   min: number;
   max: number;
}

export interface ProductAsset {
   id: string;
   preview: string;
}