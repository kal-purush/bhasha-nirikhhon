export interface IProductDetail {
  _id: string;
  product_id_hashed: string;
  product_name: string;
  product_slug: string;
  product_imgs: string[];
  product_avg_rating: {
    rating_point: number;
    rating_count: number;
  };
  product_sold_quantity: number;
  product_short_description: string;
  product_description: string;
  product_specifications: ISpecification[];
  product_variants: IVariant[];
  review_count: number[];
  recent_reviews: IReview[];
  createdAt: string;
  updatedAt: string;
}

export interface IVariant {
  variant_name: string;
  variant_slug: string;
  variant_img: string;
  variant_price: number;
  variant_stock_quantity: number;
  variant_discount_percent: number;
  _id: string;
}

export interface IReview {
  id: number;
  user: string;
  date: string;
  rating: number;
  title: string;
  content: string;
}

export interface ISpecification {
  name: string;
  value: string;
}