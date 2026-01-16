export interface ProductVariant {
  variant_name: string;
  variant_price: number;
  variant_discount_percent: number;
  discounted_price: number;
}

export interface Product {
  _id: string;
  product_name: string;
  product_slug: string;
  product_imgs: string;
  product_avg_rating: number;
  product_sold_quantity: number;
  highest_discount: number;
  product_variants: ProductVariant[];
  category: {
    _id: string;
    name: string;
  };
}

export interface IProductProps {
  product_id_hashed: string;
  product_slug: string;
  product_img: string;
  product_name: string;
  category_name: string;
  product_avg_rating: number;
  product_sold_quantity: number;
  variant_name: string[];
  product_price: number;
  lowest_price: number;
  highest_discount: number;
  show_variants?: boolean; // Add this flag
}