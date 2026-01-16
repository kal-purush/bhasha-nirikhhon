import { PayloadAction, createSlice } from "@reduxjs/toolkit";
import { Product } from "../../components/types/type";

 
type InitialState = {
  products: Product[];
  isLoading: boolean;
  searchProduct: [];

};

const initialState: InitialState = {
  products: [],
  isLoading: true,
  searchProduct: [],
};

const productSlice = createSlice({
  name: "products",
  initialState,
  reducers: {
    getProductData: (state, action: PayloadAction<Product[]>) => {
      state.products = action.payload;

    state.isLoading = false;

    },

     SearchProduct: (state, action: PayloadAction<string>) => {
      const result = state.products.filter((item) =>
        item.title.toLowerCase().includes(action.payload.toLowerCase())
      );
      state.products = result;
    },

  },
});
export const productActions = productSlice.actions;

const productReducer = productSlice.reducer;

export default productReducer;