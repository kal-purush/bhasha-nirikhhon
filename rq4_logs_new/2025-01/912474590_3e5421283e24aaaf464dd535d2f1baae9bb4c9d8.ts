import { createAsyncThunk } from "@reduxjs/toolkit";
import { API_BASE_URL, authenticatedApiCall } from "../Api/apiService.ts";

// Types
export interface DeliveryInfo {
    is_delivery: boolean;
    pickup_notes?: string;
    delivery_address?: string;
    delivery_notes?: string;
}

interface PaginationParams {
    page?: number;
    per_page?: number;
}

// Restaurant-related purchase thunks
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

// User-related purchase thunks
export const createPurchaseOrder = createAsyncThunk(
    'purchases/create',
    async (deliveryInfo: DeliveryInfo, { dispatch, rejectWithValue }) => {
        try {
            return await authenticatedApiCall(
                `${API_BASE_URL}/purchase`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(deliveryInfo)
                },
                dispatch
            );
        } catch (error) {
            return rejectWithValue(error.message);
        }
    }
);

export const fetchUserActiveOrders = createAsyncThunk(
    'purchases/fetchActiveOrders',
    async (_, { dispatch, rejectWithValue }) => {
        try {
            return await authenticatedApiCall(
                `${API_BASE_URL}/user/orders/active`,
                { method: 'GET' },
                dispatch
            );
        } catch (error) {
            return rejectWithValue(error.message);
        }
    }
);

export const fetchUserPreviousOrders = createAsyncThunk(
    'purchases/fetchPreviousOrders',
    async (params: PaginationParams, { dispatch, rejectWithValue }) => {
        try {
            const queryParams = new URLSearchParams({
                page: (params.page || 1).toString(),
                per_page: (params.per_page || 10).toString()
            });

            return await authenticatedApiCall(
                `${API_BASE_URL}/user/orders/previous?${queryParams}`,
                { method: 'GET' },
                dispatch
            );
        } catch (error) {
            return rejectWithValue(error.message);
        }
    }
);

export const fetchOrderDetails = createAsyncThunk(
    'purchases/fetchOrderDetails',
    async (purchaseId: number, { dispatch, rejectWithValue }) => {
        try {
            return await authenticatedApiCall(
                `${API_BASE_URL}/user/orders/${purchaseId}`,
                { method: 'GET' },
                dispatch
            );
        } catch (error) {
            return rejectWithValue(error.message);
        }
    }
);

// Restaurant response thunks
export const handlePurchaseResponse = createAsyncThunk(
    'purchases/respond',
    async ({ purchaseId, action }: { purchaseId: number; action: 'accept' | 'reject' },
           { dispatch, rejectWithValue }) => {
        try {
            return await authenticatedApiCall(
                `${API_BASE_URL}/purchase/${purchaseId}/response`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ action })
                },
                dispatch
            );
        } catch (error) {
            return rejectWithValue(error.message);
        }
    }
);

export const acceptPurchaseOrder = createAsyncThunk(
    'purchases/accept',
    async (purchaseId: number, { dispatch, rejectWithValue }) => {
        try {
            return await authenticatedApiCall(
                `${API_BASE_URL}/purchases/${purchaseId}/accept`,
                { method: 'POST' },
                dispatch
            );
        } catch (error) {
            console.log(error);
            return rejectWithValue(error.message);
        }
    }
);

export const rejectPurchaseOrder = createAsyncThunk(
    'purchases/reject',
    async (purchaseId: number, { dispatch, rejectWithValue }) => {
        try {
            return await authenticatedApiCall(
                `${API_BASE_URL}/purchases/${purchaseId}/reject`,
                { method: 'POST' },
                dispatch
            );
        } catch (error) {
            return rejectWithValue(error.message);
        }
    }
);
// purchaseThunks.ts
export const addCompletionImage = createAsyncThunk(
    'purchases/addImage',
    async (
        { purchaseId, file }: { purchaseId: number; file: File },
        { dispatch, rejectWithValue }
    ) => {
        try {
            const formData = new FormData();
            formData.append('file', file); // Try 'file' instead of 'completion_image'

            // Debug logging
            console.log('Uploading file:', {
                name: file.name,
                type: file.type,
                size: file.size,
                purchaseId
            });

            // Log FormData contents
            formData.forEach((value, key) => {
                if (value instanceof File) {
                    console.log('FormData entry:', key, {
                        name: value.name,
                        type: value.type,
                        size: value.size
                    });
                } else {
                    console.log('FormData entry:', key, value);
                }
            });

            return await authenticatedApiCall(
                `${API_BASE_URL}/purchase/${purchaseId}/completion-image`,
                {
                    method: 'POST',
                    body: formData,
                },
                dispatch
            );
        } catch (error) {
            console.error('Upload error:', error);
            return rejectWithValue(
                error.message && typeof error.message === 'string'
                    ? JSON.parse(error.message)
                    : 'Failed to upload image'
            );
        }
    }
);