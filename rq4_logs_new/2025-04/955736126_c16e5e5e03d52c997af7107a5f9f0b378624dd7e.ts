import { create } from "zustand";
import { apolloClient } from "@/apollo";
import { GET_PRODUCTS_BY_CATEGORY } from "@/graphql/queries";
import { Product } from "@/types";

interface ProductStore {
  products: Product[];
  loading: boolean;
  error: string | null;
  fetchProducts: (category: string) => Promise<void>;
}

export const useProductStore = create<ProductStore>((set) => ({
  products: [],
  loading: false,
  error: null,

  fetchProducts: async (category: string) => {
    set({ loading: true, error: null });

    try {
      const { data } = await apolloClient.query({
        query: GET_PRODUCTS_BY_CATEGORY, variables: { category },
      });

      set({
        products: data.products,
        loading: false,
        error: null,
      });
    } catch (err) {
      set({
        loading: false,
        error: err instanceof Error ? err.message : "Something went wrong",
      });
    }
  },
}));