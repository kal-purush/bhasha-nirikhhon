import { createSlice, createAsyncThunk } from "@reduxjs/toolkit";
import axios from "axios";


// API Configuration
const API_URL = "https://api.themoviedb.org/3";
const API_KEY = "your_api_key_here"; // Replace with your TMDB API key

// Thunk to Fetch Movie Details
export const fetchMovieDetails = createAsyncThunk(
  "movies/fetchMovieDetails",
  async (movieId, thunkAPI) => {
    try {
      const response = await axios.get(`${API_URL}/movie/${movieId}`, {
        params: { api_key: API_KEY },
      });
      return response.data;
    } catch (error) {
      return thunkAPI.rejectWithValue(error.message);
    }
  }
);

// Slice Definition
const moviesSlice = createSlice({
  name: "movies",
  initialState: {
    movies: [],
    genres: [],
    selectedMovie: null,
    favorites: [],
    loading: false,
    error: null,
  },
  reducers: {
    addFavorite: (state, action) => {
      state.favorites.push(action.payload);
    },
    removeFavorite: (state, action) => {
      state.favorites = state.favorites.filter(
        (movie) => movie.id !== action.payload.id
      );
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchMovieDetails.pending, (state) => {
        state.loading = true;
        state.selectedMovie = null;
      })
      .addCase(fetchMovieDetails.fulfilled, (state, action) => {
        state.loading = false;
        state.selectedMovie = action.payload;
      })
      .addCase(fetchMovieDetails.rejected, (state, action) => {
        state.loading = false;
        state.error = action.payload;
      });
  },
});

// Export Actions and Reducer
export const { addFavorite, removeFavorite } = moviesSlice.actions;
export default moviesSlice.reducer;