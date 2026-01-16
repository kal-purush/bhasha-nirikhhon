import { Product } from "components/ProductCard/types";

export interface CartItem {
    id: string;
    product: Product;
    amount: number;
}

export interface CartProps extends CartItem {
    addToCart: (product: Product) => void;
    decreaseFromCart: (product: Product, remove?: boolean) => void;
}