import { configureStore } from "@reduxjs/toolkit";
import { setupListeners } from "@reduxjs/toolkit/query";
import { productsApi } from "./services/ApiData";
import { CartSlice } from "./features/CartSlice";


export const Store = configureStore({
    reducer: {
        [productsApi.reducerPath]: productsApi.reducer,
        cart: CartSlice.reducer
    },
    // to help with cache (caching)
    middleware: (getDefaultMiddleware) =>
        getDefaultMiddleware().concat(productsApi.middleware),
})


setupListeners(Store.dispatch);