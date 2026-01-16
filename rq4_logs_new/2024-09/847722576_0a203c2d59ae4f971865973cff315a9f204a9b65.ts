import { create, StateCreator } from "zustand";
import { Product } from "../interface/ProductInterface";

interface CartState {
  products: Product[];
}

interface CartActions {
  addProduct: (product: Product) => void;
}

export interface CartSlice extends CartState, CartActions {}

export const createCartStore: StateCreator<CartSlice> = (set) => ({
  products: [],
  addProduct: (product) =>
    set((state) => ({ products: [...state.products, product] })),
});

// export const createCartStore = create<CartSlice>((set) => ({
//   products: [],
//   addProduct: (product) =>
//     set((state) => ({ products: [...state.products, product] })),
// }));