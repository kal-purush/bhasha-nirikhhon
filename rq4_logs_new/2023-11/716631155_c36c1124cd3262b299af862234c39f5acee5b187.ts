import { create } from "zustand";

interface CategoryState {
  categories: { [key: number]: string };
  setCategories: (updatedCategory: { [key: number]: string }) => void;
  categoryId: number;
  setCategoryId: (categoryId: number) => void;
}

const useCategoryStore = create<CategoryState>((set) => ({
  categories: {
    1: "한식",
    2: "중식",
    3: "일식",
    4: "기타 외식업",
    5: "미용",
    6: "기타",
    7: "세탁",
    8: "숙박",
  },
  setCategories: (updatedCategory: { [key: number]: string }) =>
    set((state) => ({
      categories: { ...state.categories, ...updatedCategory },
    })),
  categoryId: 1,
  setCategoryId: (categoryId) => set({ categoryId: categoryId }),
}));

export default useCategoryStore;