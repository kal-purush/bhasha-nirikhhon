import { createSlice, createAsyncThunk, PayloadAction } from "@reduxjs/toolkit";
import {
  DungeonsAndDragons,
  DungeonsAndDragonsRoot,
  IDragon,
} from "../../models/Imodel";
import axios from "axios";

const BASE_URL = "https://www.dnd5eapi.co";

interface DungeonsAndDragonsState {
  loading: boolean;
  error: string | null;
  data: DungeonsAndDragons[] | null;
  dragondata: IDragon | null;
  dragonloading: boolean;
  dragonerror: string | null;
  favorites: DungeonsAndDragons[];
  searchQuery: string;
}

const initialState: DungeonsAndDragonsState = {
  loading: false,
  error: null,
  data: null,
  dragondata: null,
  dragonloading: false,
  dragonerror: null,
  favorites: [],
  searchQuery: "",
};

export const getData = createAsyncThunk("getData", async (_, thunkApi) => {
  try {
    const response = await axios.get<DungeonsAndDragonsRoot>(
      `${BASE_URL}/api/spells`
    );
    return response.data.results;
  } catch (error: any) {
    const message = error.message;
    return thunkApi.rejectWithValue(message);
  }
});

export const getDragonData = createAsyncThunk(
  "getData/dragonData",
  async (url: string, thunkApi) => {
    console.log(url, "insidethunk");

    try {
      const response = await axios.get<DungeonsAndDragonsRoot>(
        `${BASE_URL}${url}`
      );
      return response.data;
    } catch (error: any) {
      const message = error.message;
      return thunkApi.rejectWithValue(message);
    }
  }
);

const DungeonsAndDragonsStateSlice = createSlice({
  name: "DungeonsAndDragons",
  initialState,
  reducers: {
    addToFavorites: (state, action: PayloadAction<DungeonsAndDragons>) => {
      const itemToAdd = action.payload;
      const isItemInFavorites = state.favorites.some(
        (item) => item.index === itemToAdd.index
      );

      if (!isItemInFavorites) {
        state.favorites.push(itemToAdd);
      }
    },
    removeFromFavorites(state, action: PayloadAction<string>) {
      const index = state.favorites.findIndex(
        (item) => item.name === action.payload
      );
      if (index !== -1) {
        state.favorites.splice(index, 1);
      }
    },
    updateSearchQuery(state, action: PayloadAction<string>) {
      state.searchQuery = action.payload;
    },
  },
  extraReducers(builder) {
    builder
      .addCase(getData.pending, (state, action) => {
        state.loading = true;
      })
      .addCase(
        getData.fulfilled,
        (state, action: PayloadAction<DungeonsAndDragons[]>) => {
          state.loading = false;
          state.data = action.payload.map(
            (item: DungeonsAndDragons) => (item = { ...item, favourite: false })
          );
        }
      )
      .addCase(getData.rejected, (state, action: PayloadAction<any>) => {
        state.error = action.payload;
      })
      .addCase(getDragonData.pending, (state, action) => {
        state.dragonloading = true;
      })
      .addCase(getDragonData.fulfilled, (state, action: PayloadAction<any>) => {
        state.dragonloading = false;
        state.dragondata = action.payload;
      })
      .addCase(getDragonData.rejected, (state, action: PayloadAction<any>) => {
        state.error = action.payload;
      });
  },
});

export const { addToFavorites, removeFromFavorites, updateSearchQuery } =
  DungeonsAndDragonsStateSlice.actions;
export default DungeonsAndDragonsStateSlice.reducer;