export interface OrderProduct {
  product_id: string;
  variant_id: string;
  quantity: number;
  unit_price: number;
  discount_percent: number;
  product_name: string;
  product_img: string;
  variant_name: string;
  variant_img: string;
}

export interface OrderBuyer {
  name: string;
  phone_number: string;
  address: {
    province: string;
    district: string;
    ward: string;
    street: string;
  };
}

export interface Order {
  _id: string;
  user_id: string;
  order_products: OrderProduct[];
  order_buyer: OrderBuyer;
  order_note: string;
  total_products_cost: number;
  shipping_cost: number;
  final_cost: number;
  order_status: "shipping" | "completed" | "pending" | "cancelled";
  createdAt: string | null;
}