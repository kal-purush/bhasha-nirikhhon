import type { PayloadAction } from '@reduxjs/toolkit';
import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';

import api from '../../config/axios';
import { Product } from '../../types/shop';
import { store } from '../store';
import { Toast } from 'react-native-toast-message/lib/src/Toast';
import { setProducts } from './products';


export interface ProductWithQuantity extends Product {
    quantity: number;
    checkForBuy: boolean;
}

export interface CartState {
    items: ProductWithQuantity[];
}

interface BuyItems {
    items: ProductWithQuantity[];
    total: number;
}


const initialState: CartState = {
    items: [],
}

export const cartSlice = createSlice({
    name: 'cart',
    initialState,
    reducers: {
        addItemTocart: (state, action: PayloadAction<ProductWithQuantity>) => {
            const item = state.items.find(item => item.id === action.payload.id);
            if (item) {
                item.quantity += action.payload.quantity;
            } else {
                state.items.push(action.payload);
            }
        },
        removerQuantity: (state, action: PayloadAction<ProductWithQuantity>) => {
            const item = state.items.find(item => item.id === action.payload.id);
            if (item) {
                item.quantity--;
                if (item.quantity === 0) {
                    state.items = state.items.filter(item => item.id !== action.payload.id);
                }
            }
        },
        removeItemToCart: (state, action: PayloadAction<ProductWithQuantity>) => {
            const item = state.items.find(item => item.id === action.payload.id);
            if (item) {
                item.quantity--;
                if (item.quantity === 0) {
                    state.items = state.items.filter(item => item.id !== action.payload.id);
                }
            }
        },
        updateCheckForBuy: (state, action: PayloadAction<ProductWithQuantity>) => {
            const item = state.items.find(item => item.id === action.payload.id);
            if (item) {
                item.checkForBuy = action.payload.checkForBuy;
            }
        },
        deleteAllItems: (state) => {
            state.items = [];
        },
    },
    extraReducers: (builder) => {
        builder
            .addCase(buyItems.fulfilled, (state, action) => {
                state.items = [];
            })
            .addCase(buyItems.rejected, (state, action) => {
                throw action.error;
            })
    },
});

export const buyItems = createAsyncThunk(
    'cart/buyItems',
    async (data: BuyItems) => {
        try {
            const response = await api.post('/buy', { data });
            return response.data.products;
        } catch (error) {
            throw error;
        }
    }
)


export const { addItemTocart, removeItemToCart, removerQuantity, updateCheckForBuy, deleteAllItems } = cartSlice.actions;

export default cartSlice.reducer;