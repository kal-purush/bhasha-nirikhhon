import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import {API_BASE_URL, authenticatedApiCall, getAuthHeaders} from "../Api/apiService.ts";


export interface PurchaseItem {
    id: number;
    name: string;
    quantity: number;
    price: number;
}

export interface DeliveryInfo {
    address: string;
    notes?: string;
}

export interface Purchase {
    id: number;
    date: string;
    status: 'pending' | 'accepted' | 'rejected' | 'completed';
    items: PurchaseItem[];
    total: number;
    completionImage?: string;
    userId: string;
    restaurantId: number;
    delivery_address?: string;
    delivery_notes?: string;
    is_delivery: boolean;
    listing_id: number;
    listing_title?: string;
}

interface PurchaseState {
    items: Purchase[];
    loading: boolean;
    error: string | null;
}

const initialState: PurchaseState = {
    items: [],
    loading: false,
    error: null
};

// Update your thunks to use the new utility
export const fetchRestaurantPurchases = createAsyncThunk(
    'purchases/fetchByRestaurant',
    async (restaurantId: number, { dispatch, rejectWithValue }) => {
        try {
            return await authenticatedApiCall(
                `${API_BASE_URL}/restaurant/${restaurantId}/purchases`,
                { method: 'GET' },
                dispatch
            );
        } catch (error) {
            return rejectWithValue(error.message);
        }
    }
);

export const createPurchaseOrder = createAsyncThunk(
    'purchases/create',
    async (deliveryInfo: DeliveryInfo | undefined, { dispatch, rejectWithValue }) => {
        try {
            return await authenticatedApiCall(
                `${API_BASE_URL}/purchase`,
                {
                    method: 'POST',
                    body: JSON.stringify({ delivery_info: deliveryInfo })
                },
                dispatch
            );
        } catch (error) {
            return rejectWithValue(error.message);
        }
    }
);

export const handlePurchaseResponse = createAsyncThunk(
    'purchases/respond',
    async ({ purchaseId, action }: { purchaseId: number; action: 'accept' | 'reject' }) => {
        const response = await fetch(`${API_BASE_URL}/purchase/${purchaseId}/response`, {
            method: 'POST',
            headers: getAuthHeaders(),
            body: JSON.stringify({ action })
        });
        if (!response.ok) throw new Error(`Failed to ${action} purchase`);
        return await response.json();
    }
);

export const acceptPurchaseOrder = createAsyncThunk(
    'purchases/accept',
    async (purchaseId: number) => {
        const response = await fetch(`${API_BASE_URL}/purchases/${purchaseId}/accept`, {
            method: 'POST',
            headers: getAuthHeaders()
        });
        if (!response.ok) throw new Error('Failed to accept purchase');
        return await response.json();
    }
);

export const rejectPurchaseOrder = createAsyncThunk(
    'purchases/reject',
    async (purchaseId: number) => {
        const response = await fetch(`${API_BASE_URL}/purchases/${purchaseId}/reject`, {
            method: 'POST',
            headers: getAuthHeaders()
        });
        if (!response.ok) throw new Error('Failed to reject purchase');
        return await response.json();
    }
);

export const addCompletionImage = createAsyncThunk(
    'purchases/addImage',
    async ({ purchaseId, imageUrl }: { purchaseId: number; imageUrl: string }) => {
        const response = await fetch(`${API_BASE_URL}/purchase/${purchaseId}/completion-image`, {
            method: 'POST',
            headers: getAuthHeaders(),
            body: JSON.stringify({ image_url: imageUrl })
        });
        if (!response.ok) throw new Error('Failed to add image');
        return await response.json();
    }
);

const purchaseSlice = createSlice({
    name: 'purchases',
    initialState,
    reducers: {},
    extraReducers: (builder) => {
        builder
            // Create purchase order
            .addCase(createPurchaseOrder.pending, (state) => {
                state.loading = true;
                state.error = null;
            })
            .addCase(createPurchaseOrder.fulfilled, (state, action) => {
                state.loading = false;
                state.items = [...state.items, ...action.payload.purchases];
            })
            .addCase(createPurchaseOrder.rejected, (state, action) => {
                state.loading = false;
                state.error = action.error.message || 'Failed to create purchase';
            })
            // Fetch restaurant purchases
            .addCase(fetchRestaurantPurchases.pending, (state) => {
                state.loading = true;
                state.error = null;
            })
            .addCase(fetchRestaurantPurchases.fulfilled, (state, action) => {
                state.loading = false;
                state.items = action.payload.purchases;
            })
            .addCase(fetchRestaurantPurchases.rejected, (state, action) => {
                state.loading = false;
                state.error = action.error.message || 'Failed to fetch purchases';
            })
            // Handle purchase response (accept/reject)
            .addCase(handlePurchaseResponse.fulfilled, (state, action) => {
                const index = state.items.findIndex(p => p.id === action.payload.purchase_id);
                if (index !== -1) {
                    state.items[index] = {
                        ...state.items[index],
                        status: action.payload.status
                    };
                }
            })
            // Accept purchase
            .addCase(acceptPurchaseOrder.fulfilled, (state, action) => {
                const index = state.items.findIndex(p => p.id === action.payload.purchase_id);
                if (index !== -1) {
                    state.items[index] = {
                        ...state.items[index],
                        status: action.payload.status
                    };
                }
            })
            // Reject purchase
            .addCase(rejectPurchaseOrder.fulfilled, (state, action) => {
                const index = state.items.findIndex(p => p.id === action.payload.purchase_id);
                if (index !== -1) {
                    state.items[index] = {
                        ...state.items[index],
                        status: action.payload.status
                    };
                }
            })
            // Add completion image
            .addCase(addCompletionImage.fulfilled, (state, action) => {
                const index = state.items.findIndex(p => p.id === action.payload.purchase_id);
                if (index !== -1) {
                    state.items[index] = {
                        ...state.items[index],
                        completionImage: action.payload.image_url
                    };
                }
            });
    }
});

export default purchaseSlice.reducer;