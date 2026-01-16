// store/slices/restaurantSlice.ts
import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import {
    addRestaurant,
    fetchRestaurant,
    fetchOwnedRestaurants,
    removeRestaurant,
    fetchRestaurantsProximity,
} from '../thunks/restaurantThunk';

export interface Restaurant {
    id: number;
    owner_id: number;
    restaurantName: string;
    restaurantDescription?: string;
    longitude: number;
    latitude: number;
    category: string;
    workingDays: string[];
    workingHoursStart?: string;
    workingHoursEnd?: string;
    listings: number;
    rating?: number;
    ratingCount: number;
    image_url?: string;
    distance_km?: number; // For proximity-based fetching
}

interface RestaurantState {
    ownedRestaurants: Restaurant[];
    singleRestaurant: Restaurant | null;
    nearbyRestaurants: Restaurant[];
    status: 'idle' | 'loading' | 'succeeded' | 'failed';
    error: string | null;
}

const initialState: RestaurantState = {
    ownedRestaurants: [],
    singleRestaurant: null,
    nearbyRestaurants: [],
    status: 'idle',
    error: null,
};

const restaurantSlice = createSlice({
    name: 'restaurant',
    initialState,
    reducers: {
        // You can add synchronous reducers here if needed
    },
    extraReducers: (builder) => {
        // Add Restaurant
        builder.addCase(addRestaurant.pending, (state) => {
            state.status = 'loading';
            state.error = null;
        });
        builder.addCase(addRestaurant.fulfilled, (state, action: PayloadAction<any>) => {
            state.status = 'succeeded';
            if (action.payload.restaurant) {
                state.ownedRestaurants.push(action.payload.restaurant);
            }
        });
        builder.addCase(addRestaurant.rejected, (state, action) => {
            state.status = 'failed';
            state.error = action.payload as string;
        });

        // Fetch Single Restaurant
        builder.addCase(fetchRestaurant.pending, (state) => {
            state.status = 'loading';
            state.error = null;
        });
        builder.addCase(fetchRestaurant.fulfilled, (state, action: PayloadAction<Restaurant>) => {
            state.status = 'succeeded';
            state.singleRestaurant = action.payload;
        });
        builder.addCase(fetchRestaurant.rejected, (state, action) => {
            state.status = 'failed';
            state.error = action.payload as string;
        });

        // Fetch Owned Restaurants
        builder.addCase(fetchOwnedRestaurants.pending, (state) => {
            state.status = 'loading';
            state.error = null;
        });
        builder.addCase(fetchOwnedRestaurants.fulfilled, (state, action: PayloadAction<Restaurant[]>) => {
            state.status = 'succeeded';
            state.ownedRestaurants = action.payload;
        });
        builder.addCase(fetchOwnedRestaurants.rejected, (state, action) => {
            state.status = 'failed';
            state.error = action.payload as string;
        });

        // Remove Restaurant
        builder.addCase(removeRestaurant.pending, (state) => {
            state.status = 'loading';
            state.error = null;
        });
        builder.addCase(removeRestaurant.fulfilled, (state, action: PayloadAction<{ restaurantId: number; data: any }>) => {
            state.status = 'succeeded';
            state.ownedRestaurants = state.ownedRestaurants.filter(
                (restaurant) => restaurant.id !== action.payload.restaurantId
            );
        });
        builder.addCase(removeRestaurant.rejected, (state, action) => {
            state.status = 'failed';
            state.error = action.payload as string;
        });

        // Fetch Restaurants by Proximity
        builder.addCase(fetchRestaurantsProximity.pending, (state) => {
            state.status = 'loading';
            state.error = null;
        });
        builder.addCase(fetchRestaurantsProximity.fulfilled, (state, action: PayloadAction<{ restaurants: Restaurant[] }>) => {
            state.status = 'succeeded';
            state.nearbyRestaurants = action.payload.restaurants;
        });
        builder.addCase(fetchRestaurantsProximity.rejected, (state, action) => {
            state.status = 'failed';
            state.error = action.payload as string;
        });
    },
});

export default restaurantSlice.reducer;