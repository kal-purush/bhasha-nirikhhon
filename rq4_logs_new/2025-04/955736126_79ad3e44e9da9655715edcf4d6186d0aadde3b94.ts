import { create } from "zustand";
import { apolloClient } from "@/apollo";
import { GET_CATEGORIES } from "@/graphql/queries";
import { Category } from "@/types";

interface CategoryStore {
  categories: Category[];
  fetched: boolean;
  loading: boolean;
  error: string | null;
  fetchCategories: () => Promise<void>;
}

export const useCategoryStore = create<CategoryStore>((set, get) => ({
  categories: [],
  fetched: false,
  loading: false,
  error: null,

  fetchCategories: async () => {
    if (get().fetched) return;

    set({ loading: true, error: null });

    try {
      const { data } = await apolloClient.query({
        query: GET_CATEGORIES,
      });

      set({
        categories: data.categories,
        fetched: true,
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